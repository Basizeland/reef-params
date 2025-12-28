from sqlalchemy import Column, Integer, String, Float, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from database import Base

# Helper to allow templates to access obj['key'] like the old code
class DictMixin:
    def __getitem__(self, key):
        return getattr(self, key)
    def get(self, key, default=None):
        return getattr(self, key, default)

class Tank(Base, DictMixin):
    __tablename__ = "tanks"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    volume_l = Column(Float, nullable=True)

    profile = relationship("TankProfile", uselist=False, back_populates="tank", cascade="all, delete-orphan")
    samples = relationship("Sample", back_populates="tank", order_by="desc(Sample.taken_at)", cascade="all, delete-orphan")
    targets = relationship("Target", back_populates="tank", cascade="all, delete-orphan")
    dose_logs = relationship("DoseLog", back_populates="tank", cascade="all, delete-orphan")

class TankProfile(Base, DictMixin):
    __tablename__ = "tank_profiles"
    tank_id = Column(Integer, ForeignKey("tanks.id"), primary_key=True)
    volume_l = Column(Float)
    net_percent = Column(Float, default=100)
    tank = relationship("Tank", back_populates="profile")

class ParameterDef(Base, DictMixin):
    __tablename__ = "parameter_defs"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False)
    unit = Column(String, nullable=True)
    active = Column(Integer, default=1)
    sort_order = Column(Integer, default=0)
    max_daily_change = Column(Float, nullable=True)

class Sample(Base, DictMixin):
    __tablename__ = "samples"
    id = Column(Integer, primary_key=True, index=True)
    tank_id = Column(Integer, ForeignKey("tanks.id"), nullable=False)
    taken_at = Column(String, nullable=False)
    notes = Column(String, nullable=True)

    tank = relationship("Tank", back_populates="samples")
    values = relationship("SampleValue", back_populates="sample", cascade="all, delete-orphan")

class SampleValue(Base, DictMixin):
    __tablename__ = "sample_values"
    sample_id = Column(Integer, ForeignKey("samples.id"), primary_key=True)
    parameter_id = Column(Integer, ForeignKey("parameter_defs.id"), primary_key=True)
    value = Column(Float)

    sample = relationship("Sample", back_populates="values")
    parameter_def = relationship("ParameterDef")

class Target(Base, DictMixin):
    __tablename__ = "targets"
    id = Column(Integer, primary_key=True)
    tank_id = Column(Integer, ForeignKey("tanks.id"), nullable=False)
    parameter = Column(String, nullable=False)
    target_low = Column(Float)
    target_high = Column(Float)
    alert_low = Column(Float)
    alert_high = Column(Float)
    unit = Column(String)
    enabled = Column(Integer, default=1)
    tank = relationship("Tank", back_populates="targets")

class TestKit(Base, DictMixin):
    __tablename__ = "test_kits"
    id = Column(Integer, primary_key=True)
    parameter = Column(String) 
    name = Column(String)
    unit = Column(String)
    resolution = Column(Float)
    min_value = Column(Float)
    max_value = Column(Float)
    notes = Column(String)
    active = Column(Integer, default=1)

class Additive(Base, DictMixin):
    __tablename__ = "additives"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    parameter = Column(String)
    strength = Column(Float)
    unit = Column(String)
    max_daily = Column(Float)
    notes = Column(String)
    active = Column(Integer, default=1)

class Preset(Base, DictMixin):
    __tablename__ = "presets"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)
    items = relationship("PresetItem", back_populates="preset", cascade="all, delete-orphan")

class PresetItem(Base, DictMixin):
    __tablename__ = "preset_items"
    id = Column(Integer, primary_key=True)
    preset_id = Column(Integer, ForeignKey("presets.id"))
    additive_name = Column(String)
    parameter = Column(String)
    strength = Column(Float)
    unit = Column(String)
    max_daily = Column(Float)
    notes = Column(String)
    preset = relationship("Preset", back_populates="items")

class DoseLog(Base, DictMixin):
    __tablename__ = "dose_logs"
    id = Column(Integer, primary_key=True)
    tank_id = Column(Integer, ForeignKey("tanks.id"))
    additive_id = Column(Integer, ForeignKey("additives.id"), nullable=True)
    amount_ml = Column(Float)
    reason = Column(String)
    logged_at = Column(String)
    tank = relationship("Tank", back_populates="dose_logs")
    additive = relationship("Additive")

class DosePlanCheck(Base, DictMixin):
    __tablename__ = "dose_plan_checks"
    id = Column(Integer, primary_key=True)
    tank_id = Column(Integer, ForeignKey("tanks.id"))
    parameter = Column(String)
    additive_id = Column(Integer)
    planned_date = Column(String)
    checked = Column(Integer, default=0)
    checked_at = Column(String)
    __table_args__ = (UniqueConstraint('tank_id', 'parameter', 'additive_id', 'planned_date', name='_tank_param_add_date_uc'),)
