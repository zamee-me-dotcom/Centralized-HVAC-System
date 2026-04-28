"""
Authentication & Authorization Middleware
=========================================
Provides:

1. JWT Bearer token verification (RS256 / HS256)
2. Role-based access control (RBAC) with permission scopes
3. API key authentication for device/service accounts
4. Rate limiting per identity (Redis token bucket)
5. Request audit logging

Roles
-----
  admin       – full system access
  operator    – read + issue commands
  viewer      – read-only
  device      – telemetry publish only (for edge gateways calling REST)
  service     – inter-service communication (bypasses user rate limits)

Usage
-----
    from backend.shared.auth import require_role, Roles

    @app.get("/units")
    async def list_units(user=Depends(require_role(Roles.VIEWER))):
        ...

    @app.post("/units/{id}/command")
    async def command(user=Depends(require_role(Roles.OPERATOR))):
        ...
"""
from __future__ import annotations

import hashlib
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache
from typing import List, Optional, Set

import redis.asyncio as aioredis
from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBearer,
    APIKeyHeader,
)
from jose import JWTError, jwt
from pydantic import BaseModel

from .config import get_settings

log = logging.getLogger(__name__)
settings = get_settings()

# ── Security schemes ──────────────────────────────────────────────────────────
bearer_scheme = HTTPBearer(auto_error=False)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


# ── RBAC definitions ──────────────────────────────────────────────────────────

class Roles:
    ADMIN    = "admin"
    OPERATOR = "operator"
    VIEWER   = "viewer"
    DEVICE   = "device"
    SERVICE  = "service"


# Permission matrix: role → set of allowed scopes
ROLE_PERMISSIONS: dict[str, Set[str]] = {
    Roles.ADMIN:    {"units:read", "units:write", "units:delete", "commands:write",
                     "alerts:read", "alerts:write", "zones:read", "zones:write",
                     "rules:write", "twins:read", "telemetry:read", "admin:all"},
    Roles.OPERATOR: {"units:read", "units:write", "commands:write",
                     "alerts:read", "alerts:write", "zones:read", "twins:read",
                     "telemetry:read"},
    Roles.VIEWER:   {"units:read", "alerts:read", "zones:read", "twins:read",
                     "telemetry:read"},
    Roles.DEVICE:   {"telemetry:write", "heartbeat:write"},
    Roles.SERVICE:  {"units:read", "units:write", "commands:write", "alerts:write",
                     "twins:read", "twins:write", "telemetry:read"},
}


# ── Token models ──────────────────────────────────────────────────────────────

class TokenPayload(BaseModel):
    sub: str                        # subject: user_id or device_id
    roles: List[str]
    exp: int
    iat: int
    jti: str = ""                   # JWT ID for revocation
    service: bool = False           # inter-service token


class AuthenticatedUser(BaseModel):
    id: str
    roles: List[str]
    permissions: Set[str]
    is_service: bool = False

    @classmethod
    def from_payload(cls, payload: TokenPayload) -> "AuthenticatedUser":
        permissions: Set[str] = set()
        for role in payload.roles:
            permissions.update(ROLE_PERMISSIONS.get(role, set()))
        return cls(
            id=payload.sub,
            roles=payload.roles,
            permissions=permissions,
            is_service=payload.service,
        )

    def has_permission(self, scope: str) -> bool:
        return scope in self.permissions or "admin:all" in self.permissions


# ── Token creation (for login endpoint / service bootstrap) ───────────────────

def create_access_token(
    subject: str,
    roles: List[str],
    expires_delta: Optional[timedelta] = None,
    is_service: bool = False,
) -> str:
    now = datetime.now(timezone.utc)
    exp = now + (expires_delta or timedelta(minutes=settings.JWT_EXPIRE_MINUTES))
    payload = {
        "sub":     subject,
        "roles":   roles,
        "iat":     int(now.timestamp()),
        "exp":     int(exp.timestamp()),
        "jti":     str(uuid.uuid4()),
        "service": is_service,
    }
    return jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)


# ── API key store (in production: store hashed keys in PostgreSQL) ────────────

# Hash → (user_id, roles) mapping. Populate from DB at startup.
_API_KEY_STORE: dict[str, tuple[str, List[str]]] = {}


def register_api_key(raw_key: str, user_id: str, roles: List[str]) -> str:
    """Register an API key. Returns the key hash for storage."""
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
    _API_KEY_STORE[key_hash] = (user_id, roles)
    return key_hash


# Pre-register a default service key for inter-service comms
_INTERNAL_SERVICE_KEY = "hvac-internal-service-key-changeme"
register_api_key(_INTERNAL_SERVICE_KEY, "internal-service", [Roles.SERVICE])


# ── Rate limiter (token bucket via Redis) ─────────────────────────────────────

class RateLimiter:
    """
    Sliding-window rate limiter backed by Redis.
    Service accounts and admins bypass rate limits.
    """
    DEFAULT_RATE  = 100   # requests per window
    DEFAULT_WINDOW = 60   # seconds

    def __init__(self, redis: aioredis.Redis):
        self._redis = redis

    async def check(self, identity: str, rate: int = DEFAULT_RATE,
                    window: int = DEFAULT_WINDOW) -> bool:
        key   = f"ratelimit:{identity}"
        now   = time.time()
        pipe  = self._redis.pipeline()
        pipe.zremrangebyscore(key, 0, now - window)
        pipe.zadd(key, {str(uuid.uuid4()): now})
        pipe.zcard(key)
        pipe.expire(key, window)
        results = await pipe.execute()
        count = results[2]
        return count <= rate


_rate_limiter: Optional[RateLimiter] = None

def set_rate_limiter(redis: aioredis.Redis) -> None:
    global _rate_limiter
    _rate_limiter = RateLimiter(redis)


# ── Core verification ─────────────────────────────────────────────────────────

async def _verify_jwt(token: str) -> AuthenticatedUser:
    try:
        payload_dict = jwt.decode(
            token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM]
        )
        payload = TokenPayload(**payload_dict)
    except JWTError as exc:
        log.warning("JWT verification failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return AuthenticatedUser.from_payload(payload)


async def _verify_api_key(raw_key: str) -> AuthenticatedUser:
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
    entry = _API_KEY_STORE.get(key_hash)
    if not entry:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
    user_id, roles = entry
    is_service = Roles.SERVICE in roles
    permissions: Set[str] = set()
    for role in roles:
        permissions.update(ROLE_PERMISSIONS.get(role, set()))
    return AuthenticatedUser(id=user_id, roles=roles,
                             permissions=permissions, is_service=is_service)


async def get_current_user(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Security(bearer_scheme),
    api_key: Optional[str] = Security(api_key_header),
) -> AuthenticatedUser:
    """
    Dependency: extracts and verifies auth from either Bearer JWT or X-API-Key.
    Applies rate limiting unless the identity is a service account.
    """
    user: Optional[AuthenticatedUser] = None

    if credentials and credentials.scheme.lower() == "bearer":
        user = await _verify_jwt(credentials.credentials)
    elif api_key:
        user = await _verify_api_key(api_key)
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Rate limiting (skip for service accounts and admins)
    if _rate_limiter and not user.is_service and Roles.ADMIN not in user.roles:
        allowed = await _rate_limiter.check(user.id)
        if not allowed:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded",
            )

    # Audit log
    log.info("AUTH user=%s roles=%s path=%s method=%s",
             user.id, user.roles, request.url.path, request.method)

    return user


def require_role(*required_roles: str):
    """
    Dependency factory. Pass one or more roles; user must have at least one.

    Example:
        @app.post("/command")
        async def cmd(user=Depends(require_role(Roles.OPERATOR, Roles.ADMIN))):
    """
    async def _check(user: AuthenticatedUser = Depends(get_current_user)):
        if not any(r in user.roles for r in required_roles):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Required role(s): {required_roles}. Your roles: {user.roles}",
            )
        return user
    return _check


def require_permission(scope: str):
    """
    Dependency factory. Checks a fine-grained permission scope.

    Example:
        @app.delete("/units/{id}")
        async def del_unit(user=Depends(require_permission("units:delete"))):
    """
    async def _check(user: AuthenticatedUser = Depends(get_current_user)):
        if not user.has_permission(scope):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied. Required: {scope}",
            )
        return user
    return _check


# ── Auth router (login endpoint) ──────────────────────────────────────────────

from fastapi import APIRouter
from pydantic import BaseModel as BM

auth_router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BM):
    username: str
    password: str


class TokenResponse(BM):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


# Static user store (replace with DB lookup + bcrypt in production)
_USER_STORE = {
    "admin":    ("admin_password_changeme",   [Roles.ADMIN]),
    "operator": ("operator_password_changeme", [Roles.OPERATOR]),
    "viewer":   ("viewer_password_changeme",   [Roles.VIEWER]),
}


@auth_router.post("/token", response_model=TokenResponse)
async def login(body: LoginRequest):
    entry = _USER_STORE.get(body.username)
    if not entry or entry[0] != body.password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )
    _, roles = entry
    token = create_access_token(body.username, roles)
    return TokenResponse(
        access_token=token,
        expires_in=settings.JWT_EXPIRE_MINUTES * 60,
    )


@auth_router.get("/me")
async def whoami(user: AuthenticatedUser = Depends(get_current_user)):
    return {
        "id":          user.id,
        "roles":       user.roles,
        "permissions": sorted(user.permissions),
        "is_service":  user.is_service,
    }
