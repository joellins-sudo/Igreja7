# app/utils.py
from __future__ import annotations
from datetime import date
from typing import Any

# Tenta usar Babel; se não houver, usa fallback simples.
try:
    from babel.numbers import format_currency as _fmt_cur
    from babel.dates import format_date as _fmt_date
    _HAS_BABEL = True
except Exception:
    _HAS_BABEL = False

def format_currency(value: Any) -> str:
    """R$ 1.234,56 em pt-BR."""
    try:
        v = float(value)
    except Exception:
        return str(value)
    if _HAS_BABEL:
        return _fmt_cur(v, "BRL", locale="pt_BR")
    return ("R$ " + f"{v:,.2f}").replace(",", "X").replace(".", ",").replace("X", ".")

def format_date(d: date) -> str:
    """dd/mm/aaaa em pt-BR."""
    if _HAS_BABEL:
        try:
            return _fmt_date(d, format="short", locale="pt_BR")
        except Exception:
            pass
    return d.strftime("%d/%m/%Y")
