# app/models.py
from datetime import datetime, date
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, String, Date, DateTime, Numeric, ForeignKey
from .db import Base

# Tipos: "DOAÇÃO" | "SAÍDA"  (compatível com "RECEITA"/"DESPESA" antigos)

class Congregation(Base):
    __tablename__ = "congregations"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)

class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String, nullable=False)
    role: Mapped[str] = mapped_column(String, nullable=False)  # SEDE | TESOUREIRO
    congregation_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("congregations.id"))
    congregation = relationship("Congregation")

class Fund(Base):
    __tablename__ = "funds"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)

class Category(Base):
    __tablename__ = "categories"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)  # DOAÇÃO | SAÍDA

class Transaction(Base):
    __tablename__ = "transactions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    date: Mapped[date] = mapped_column(Date, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)  # DOAÇÃO | SAÍDA
    category_id: Mapped[int] = mapped_column(Integer, ForeignKey("categories.id"), nullable=False)
    fund_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("funds.id"))
    amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    description: Mapped[str | None] = mapped_column(String)
    congregation_id: Mapped[int] = mapped_column(Integer, ForeignKey("congregations.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    congregation = relationship("Congregation")
    category = relationship("Category")
    fund = relationship("Fund")

# Dízimo individual (para totalizar dizimistas/valores por congregação)
class Tithe(Base):
    __tablename__ = "tithes"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    date: Mapped[date] = mapped_column(Date, nullable=False)
    tither_name: Mapped[str] = mapped_column(String, nullable=False)
    amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    congregation_id: Mapped[int] = mapped_column(Integer, ForeignKey("congregations.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    congregation = relationship("Congregation")
