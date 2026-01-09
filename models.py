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
    sort_order = Column(Integer, nullable=True)
    owner_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)

    profile = relationship("TankProfile", uselist=False, back_populates="tank", cascade="all, delete-orphan")
    samples = relationship("Sample", back_populates="tank", order_by="desc(Sample.taken_at)", cascade="all, delete-orphan")
    targets = relationship("Target", back_populates="tank", cascade="all, delete-orphan")
    dose_logs = relationship("DoseLog", back_populates="tank", cascade="all, delete-orphan")
    maintenance_tasks = relationship("TankMaintenanceTask", back_populates="tank", cascade="all, delete-orphan")
    journal_entries = relationship("TankJournal", back_populates="tank", cascade="all, delete-orphan")

class User(Base, DictMixin):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    email = Column(String, nullable=False, unique=True)
    username = Column(String, nullable=True)
    role = Column(String, nullable=True)
    password_hash = Column(String, nullable=True)
    password_salt = Column(String, nullable=True)
    google_sub = Column(String, nullable=True)
    last_login_at = Column(String, nullable=True)
    is_admin = Column(Integer, default=0)
    created_at = Column(String, nullable=False)
    admin = Column(Integer, default=0)

    tanks = relationship("Tank", backref="owner")
    sessions = relationship("Session", back_populates="user", cascade="all, delete-orphan")
    api_tokens = relationship("ApiToken", back_populates="user", cascade="all, delete-orphan")

class Session(Base, DictMixin):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    session_token = Column(String, nullable=False, unique=True)
    created_at = Column(String, nullable=False)
    expires_at = Column(String, nullable=True)
    user = relationship("User", back_populates="sessions")

class UserTank(Base, DictMixin):
    __tablename__ = "user_tanks"
    user_id = Column(Integer, ForeignKey("users.id"), primary_key=True)
    tank_id = Column(Integer, ForeignKey("tanks.id"), primary_key=True)

class TankProfile(Base, DictMixin):
    __tablename__ = "tank_profiles"
    tank_id = Column(Integer, ForeignKey("tanks.id"), primary_key=True)
    volume_l = Column(Float)
    net_percent = Column(Float, default=100)
    alk_solution = Column(String)
    alk_daily_ml = Column(Float)
    ca_solution = Column(String)
    ca_daily_ml = Column(Float)
    mg_solution = Column(String)
    mg_daily_ml = Column(Float)
    dosing_mode = Column(String)
    all_in_one_solution = Column(String)
    all_in_one_daily_ml = Column(Float)
    nitrate_solution = Column(String)
    nitrate_daily_ml = Column(Float)
    phosphate_solution = Column(String)
    phosphate_daily_ml = Column(Float)
    trace_solution = Column(String)
    trace_daily_ml = Column(Float)
    nopox_daily_ml = Column(Float)
    calcium_reactor_daily_ml = Column(Float)
    calcium_reactor_effluent_dkh = Column(Float)
    kalk_solution = Column(String)
    kalk_daily_ml = Column(Float)
    use_all_in_one = Column(Integer)
    use_alk = Column(Integer)
    use_ca = Column(Integer)
    use_mg = Column(Integer)
    use_nitrate = Column(Integer)
    use_phosphate = Column(Integer)
    use_trace = Column(Integer)
    use_nopox = Column(Integer)
    use_calcium_reactor = Column(Integer)
    use_kalkwasser = Column(Integer)
    all_in_one_container_ml = Column(Float)
    all_in_one_remaining_ml = Column(Float)
    alk_container_ml = Column(Float)
    alk_remaining_ml = Column(Float)
    ca_container_ml = Column(Float)
    ca_remaining_ml = Column(Float)
    mg_container_ml = Column(Float)
    mg_remaining_ml = Column(Float)
    nitrate_container_ml = Column(Float)
    nitrate_remaining_ml = Column(Float)
    phosphate_container_ml = Column(Float)
    phosphate_remaining_ml = Column(Float)
    trace_container_ml = Column(Float)
    trace_remaining_ml = Column(Float)
    nopox_container_ml = Column(Float)
    nopox_remaining_ml = Column(Float)
    kalk_container_ml = Column(Float)
    kalk_remaining_ml = Column(Float)
    dosing_container_updated_at = Column(String)
    dosing_low_days = Column(Float)
    tank = relationship("Tank", back_populates="profile")

class ParameterDef(Base, DictMixin):
    __tablename__ = "parameter_defs"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False)
    chemical_symbol = Column(String, nullable=True)
    unit = Column(String, nullable=True)
    active = Column(Integer, default=1)
    sort_order = Column(Integer, default=0)
    max_daily_change = Column(Float, nullable=True)
    test_interval_days = Column(Integer, nullable=True)
    default_target_low = Column(Float, nullable=True)
    default_target_high = Column(Float, nullable=True)
    default_alert_low = Column(Float, nullable=True)
    default_alert_high = Column(Float, nullable=True)

class UserParameterSetting(Base, DictMixin):
    __tablename__ = "user_parameter_settings"
    user_id = Column(Integer, ForeignKey("users.id"), primary_key=True)
    parameter_id = Column(Integer, ForeignKey("parameter_defs.id"), primary_key=True)
    max_daily_change = Column(Float, nullable=True)
    test_interval_days = Column(Integer, nullable=True)
    default_target_low = Column(Float, nullable=True)
    default_target_high = Column(Float, nullable=True)
    default_alert_low = Column(Float, nullable=True)
    default_alert_high = Column(Float, nullable=True)

class Sample(Base, DictMixin):
    __tablename__ = "samples"
    id = Column(Integer, primary_key=True, index=True)
    tank_id = Column(Integer, ForeignKey("tanks.id"), nullable=False)
    taken_at = Column(String, nullable=False)
    notes = Column(String, nullable=True)

    tank = relationship("Tank", back_populates="samples")
    values = relationship("SampleValue", back_populates="sample", cascade="all, delete-orphan")

    # --- NEW HELPER PROPERTY ---
    @property
    def values_dict(self):
        """Returns a dict of {parameter_id: value} for easy lookup in templates."""
        return {v.parameter_id: v.value for v in self.values}

class SampleValue(Base, DictMixin):
    __tablename__ = "sample_values"
    sample_id = Column(Integer, ForeignKey("samples.id"), primary_key=True)
    parameter_id = Column(Integer, ForeignKey("parameter_defs.id"), primary_key=True)
    value = Column(Float)

    sample = relationship("Sample", back_populates="values")
    parameter_def = relationship("ParameterDef")

class SampleValueKit(Base, DictMixin):
    __tablename__ = "sample_value_kits"
    sample_id = Column(Integer, ForeignKey("samples.id"), primary_key=True)
    parameter_id = Column(Integer, ForeignKey("parameter_defs.id"), primary_key=True)
    test_kit_id = Column(Integer, ForeignKey("test_kits.id"))

class Parameter(Base, DictMixin):
    __tablename__ = "parameters"
    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey("samples.id"), nullable=False)
    name = Column(String, nullable=False)
    value = Column(Float)
    unit = Column(String)
    test_kit_id = Column(Integer)

class Target(Base, DictMixin):
    __tablename__ = "targets"
    id = Column(Integer, primary_key=True)
    tank_id = Column(Integer, ForeignKey("tanks.id"), nullable=False)
    parameter = Column(String, nullable=False)
    low = Column(Float)
    high = Column(Float)
    target_low = Column(Float)
    target_high = Column(Float)
    alert_low = Column(Float)
    alert_high = Column(Float)
    unit = Column(String)
    enabled = Column(Integer, default=1)
    tank = relationship("Tank", back_populates="targets")
    __table_args__ = (UniqueConstraint("tank_id", "parameter", name="_targets_tank_parameter_uc"),)

class TestKit(Base, DictMixin):
    __tablename__ = "test_kits"
    id = Column(Integer, primary_key=True)
    parameter = Column(String) 
    name = Column(String)
    unit = Column(String)
    resolution = Column(Float)
    manufacturer_accuracy = Column(Float)
    min_value = Column(Float)
    max_value = Column(Float)
    notes = Column(String)
    conversion_type = Column(String)
    conversion_data = Column(String)
    workflow_data = Column(String)
    active = Column(Integer, default=1)

class Additive(Base, DictMixin):
    __tablename__ = "additives"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    brand = Column(String)
    group_name = Column(String)
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

class DosingEntry(Base, DictMixin):
    __tablename__ = "dosing_entries"
    id = Column(Integer, primary_key=True)
    tank_id = Column(Integer, ForeignKey("tanks.id"), nullable=False)
    parameter = Column(String, nullable=False)
    solution = Column(String)
    daily_ml = Column(Float)
    container_ml = Column(Float)
    remaining_ml = Column(Float)
    active = Column(Integer, default=1)
    created_at = Column(String, nullable=False)

class DosingNotification(Base, DictMixin):
    __tablename__ = "dosing_notifications"
    id = Column(Integer, primary_key=True)
    tank_id = Column(Integer, ForeignKey("tanks.id"), nullable=False)
    container_key = Column(String, nullable=False)
    notified_on = Column(String, nullable=False)
    dismissed_at = Column(String, nullable=True)

class AuditLog(Base, DictMixin):
    __tablename__ = "audit_logs"
    id = Column(Integer, primary_key=True)
    actor_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    action = Column(String, nullable=False)
    details = Column(String)
    created_at = Column(String, nullable=False)

class ApiToken(Base, DictMixin):
    __tablename__ = "api_tokens"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    token_hash = Column(String, nullable=False, unique=True)
    token_prefix = Column(String)
    label = Column(String)
    created_at = Column(String, nullable=False)
    last_used_at = Column(String)
    user = relationship("User", back_populates="api_tokens")

class PushSubscription(Base, DictMixin):
    __tablename__ = "push_subscriptions"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    endpoint = Column(String, nullable=False)
    subscription_json = Column(String, nullable=False)
    created_at = Column(String, nullable=False)

    __table_args__ = (UniqueConstraint("user_id", "endpoint", name="_push_subscription_user_endpoint_uc"),)

class AppSetting(Base, DictMixin):
    __tablename__ = "app_settings"
    key = Column(String, primary_key=True)
    value = Column(String)

class IcpRecommendation(Base, DictMixin):
    __tablename__ = "icp_recommendations"
    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey("samples.id"), nullable=False)
    category = Column(String, nullable=False)
    label = Column(String)
    value = Column(Float)
    unit = Column(String)
    notes = Column(String)
    severity = Column(String)

class IcpDoseCheck(Base, DictMixin):
    __tablename__ = "icp_dose_checks"
    id = Column(Integer, primary_key=True)
    sample_id = Column(Integer, ForeignKey("samples.id"), nullable=False)
    label = Column(String, nullable=False)
    checked = Column(Integer, default=0)
    checked_at = Column(String)
    __table_args__ = (UniqueConstraint("sample_id", "label", name="_icp_dose_checks_uc"),)

class TankJournal(Base, DictMixin):
    __tablename__ = "tank_journal"
    id = Column(Integer, primary_key=True)
    tank_id = Column(Integer, ForeignKey("tanks.id"), nullable=False)
    entry_date = Column(String, nullable=False)
    entry_type = Column(String)
    title = Column(String)
    notes = Column(String)
    tank = relationship("Tank", back_populates="journal_entries")

class TankMaintenanceTask(Base, DictMixin):
    __tablename__ = "tank_maintenance_tasks"
    id = Column(Integer, primary_key=True)
    tank_id = Column(Integer, ForeignKey("tanks.id"), nullable=False)
    title = Column(String, nullable=False)
    interval_days = Column(Integer, nullable=False)
    next_due_at = Column(String, nullable=False)
    last_completed_at = Column(String)
    notes = Column(String)
    active = Column(Integer, default=1)
    created_at = Column(String, nullable=False)
    tank = relationship("Tank", back_populates="maintenance_tasks")
