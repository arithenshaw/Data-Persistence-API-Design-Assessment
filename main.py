import asyncio
import os
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from sqlalchemy import String, Integer, Float, DateTime, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from uuid_extensions import uuid7str

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./profiles.db")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

engine = create_async_engine(DATABASE_URL)
Session = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class Profile(Base):
    __tablename__ = "profiles"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, index=True)
    gender: Mapped[str] = mapped_column(String)
    gender_probability: Mapped[float] = mapped_column(Float)
    sample_size: Mapped[int] = mapped_column(Integer)
    age: Mapped[int] = mapped_column(Integer)
    age_group: Mapped[str] = mapped_column(String)
    country_id: Mapped[str] = mapped_column(String)
    country_probability: Mapped[float] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


def full(p: Profile) -> dict:
    return {
        "id": p.id, "name": p.name, "gender": p.gender,
        "gender_probability": p.gender_probability, "sample_size": p.sample_size,
        "age": p.age, "age_group": p.age_group, "country_id": p.country_id,
        "country_probability": p.country_probability,
        "created_at": p.created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def slim(p: Profile) -> dict:
    return {"id": p.id, "name": p.name, "gender": p.gender,
            "age": p.age, "age_group": p.age_group, "country_id": p.country_id}


def err(status: int, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"status": "error", "message": message})


def age_group(age: int) -> str:
    if age <= 12: return "child"
    if age <= 19: return "teenager"
    if age <= 59: return "adult"
    return "senior"


app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@app.post("/api/profiles")
async def create(request: Request):
    try:
        body = await request.json()
    except Exception:
        return err(400, "Invalid JSON body")

    if not isinstance(body, dict) or "name" not in body:
        return err(400, "Missing 'name' field")
    name = body["name"]
    if not isinstance(name, str):
        return err(422, "'name' must be a string")
    if not name.strip():
        return err(400, "'name' cannot be empty")

    name = name.strip().lower()

    async with Session() as s:
        existing = await s.scalar(select(Profile).where(Profile.name == name))
        if existing:
            return JSONResponse(status_code=200, content={"status": "success", "message": "Profile already exists", "data": full(existing)})

    try:
        async with httpx.AsyncClient(timeout=10) as c:
            gen_r, age_r, nat_r = await asyncio.gather(
                c.get("https://api.genderize.io", params={"name": name}),
                c.get("https://api.agify.io", params={"name": name}),
                c.get("https://api.nationalize.io", params={"name": name}),
            )
    except Exception:
        return err(502, "Upstream request failed")

    if gen_r.status_code >= 400: return err(502, "Genderize returned an invalid response")
    if age_r.status_code >= 400: return err(502, "Agify returned an invalid response")
    if nat_r.status_code >= 400: return err(502, "Nationalize returned an invalid response")

    gen, agi, nat = gen_r.json(), age_r.json(), nat_r.json()

    if gen.get("gender") is None or not gen.get("count"):
        return err(502, "Genderize returned an invalid response")
    if agi.get("age") is None:
        return err(502, "Agify returned an invalid response")
    countries = nat.get("country") or []
    if not countries:
        return err(502, "Nationalize returned an invalid response")

    top = max(countries, key=lambda c: c.get("probability", 0))
    age = int(agi["age"])

    p = Profile(
        id=uuid7str(), name=name,
        gender=gen["gender"], gender_probability=float(gen["probability"]),
        sample_size=int(gen["count"]),
        age=age, age_group=age_group(age),
        country_id=top["country_id"], country_probability=float(top["probability"]),
        created_at=datetime.now(timezone.utc),
    )

    async with Session() as s:
        s.add(p)
        try:
            await s.commit()
        except Exception:
            await s.rollback()
            existing = await s.scalar(select(Profile).where(Profile.name == name))
            if existing:
                return JSONResponse(status_code=200, content={"status": "success", "message": "Profile already exists", "data": full(existing)})
            return err(500, "Failed to save profile")

    return JSONResponse(status_code=201, content={"status": "success", "data": full(p)})


@app.get("/api/profiles")
async def list_all(gender: Optional[str] = None, country_id: Optional[str] = None, age_group: Optional[str] = None):
    stmt = select(Profile)
    if gender: stmt = stmt.where(Profile.gender == gender.lower())
    if country_id: stmt = stmt.where(Profile.country_id == country_id.upper())
    if age_group: stmt = stmt.where(Profile.age_group == age_group.lower())
    async with Session() as s:
        rows = (await s.scalars(stmt)).all()
    data = [slim(p) for p in rows]
    return {"status": "success", "count": len(data), "data": data}


@app.get("/api/profiles/{profile_id}")
async def get_one(profile_id: str):
    async with Session() as s:
        p = await s.scalar(select(Profile).where(Profile.id == profile_id))
    if not p: return err(404, "Profile not found")
    return {"status": "success", "data": full(p)}


@app.delete("/api/profiles/{profile_id}")
async def remove(profile_id: str):
    async with Session() as s:
        p = await s.scalar(select(Profile).where(Profile.id == profile_id))
        if not p: return err(404, "Profile not found")
        await s.delete(p)
        await s.commit()
    return Response(status_code=204)