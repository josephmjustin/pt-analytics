from sqlalchemy.orm import DeclarativeBase, Mapped, relationship, mapped_column
from sqlalchemy import Integer, String, Float, Boolean, ForeignKey, DateTime
from datetime import datetime

class Base(DeclarativeBase):
    pass

class TxcStop(Base):	
    __tablename__ = "txc_stops"

    naptan_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    stop_name: Mapped[str] = mapped_column(String, nullable=False)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)

    pattern_stops: Mapped[list["TxcPatternStops"]] = relationship(back_populates="stop")


class TxcRoutePatterns(Base):
    __tablename__ = "txc_route_patterns"

    pattern_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    route_name: Mapped[str] = mapped_column(String, nullable=False)
    operator_name: Mapped[str] = mapped_column(String, nullable=False)
    direction: Mapped[str | None] = mapped_column(String, nullable=True)
    origin: Mapped[str | None] = mapped_column(String, nullable=True)
    destination: Mapped[str | None] = mapped_column(String, nullable=True)

    route_pattern_stops: Mapped[list["TxcPatternStops"]] = relationship(back_populates="route_patterns")


class TxcPatternStops(Base):
    __tablename__ = "txc_pattern_stops"

    pattern_id: Mapped[int] = mapped_column(ForeignKey("txc_route_patterns.pattern_id"), primary_key=True)
    naptan_id: Mapped[str] = mapped_column(String(20), ForeignKey("txc_stops.naptan_id"), primary_key=True)
    stop_sequence: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime)

    route_patterns: Mapped["TxcRoutePatterns"] = relationship(back_populates="route_pattern_stops")
    stop: Mapped["TxcStop"] = relationship(back_populates="pattern_stops")

class VehiclePositions(Base):
    __tablename__ = "vehicle_positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    vehicle_id: Mapped[str] = mapped_column(String, nullable=False)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    route_id: Mapped[str | None] = mapped_column(String, nullable=True)
    trip_id: Mapped[str | None] = mapped_column(String, nullable=True)
    bearing: Mapped[int] = mapped_column(Integer, nullable=True)
    analyzed: Mapped[bool | None] = mapped_column(Boolean, nullable=True, default=False)
    route_name: Mapped[str | None] = mapped_column(String, nullable=True)
    direction: Mapped[str | None] = mapped_column(String, nullable=True)
    operator: Mapped[str | None] = mapped_column(String, nullable=True)
    origin: Mapped[str | None] = mapped_column(String, nullable=True)
    destination: Mapped[str | None] = mapped_column(String, nullable=True)
