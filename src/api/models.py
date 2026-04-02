from sqlalchemy.orm import DeclarativeBase, Mapped, relationship, mapped_column
from sqlalchemy import Integer, String, Float, ForeignKey

class Base(DeclarativeBase):
    pass

class TxcStop(Base):	
    __tablename__ = "txc_stops"

    naptan_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    stop_name: Mapped[str] = mapped_column(String, nullable=False)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)

class TxcRoutePatterns(Base):
    __tablename__ = "txc_route_patterns"

    pattern_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    route_name: Mapped[str] = mapped_column(String, nullable=False)
    operator_name: Mapped[str] = mapped_column(String, nullable=False)
    direction: Mapped[str | None] = mapped_column(String, nullable=True)
    origin: Mapped[str | None] = mapped_column(String, nullable=True)
    destination: Mapped[str | None] = mapped_column(String, nullable=True)

    pattern_stops: Mapped[list["TxcPatternStops"]] = relationship(back_populates="route_patterns")


class TxcPatternStops(Base):
    __tablename__ = "txc_pattern_stops"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pattern_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("txc_route_patterns.pattern_id"),
        nullable=False
    )
    naptan_id: Mapped[str] = mapped_column(
        String(20),
        ForeignKey("txc_stops.naptan_id"),
        nullable=False
    )
    stop_sequence: Mapped[int] = mapped_column(Integer, nullable=False)

    route_patterns: Mapped["TxcRoutePatterns"] = relationship(back_populates="pattern_stops")
