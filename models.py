from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, Numeric, String, Text, Float
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db import Base


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class Customer(TimestampMixin, Base):
    __tablename__ = "customers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    display_name: Mapped[Optional[str]] = mapped_column(String(100))
    line_user_id: Mapped[Optional[str]] = mapped_column(String(255), unique=True, index=True)
    email: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    phone: Mapped[Optional[str]] = mapped_column(String(50))

    orders: Mapped[list[Order]] = relationship(back_populates="customer")
    yaml_logs: Mapped[list[YamlLog]] = relationship(back_populates="customer")


class Menu(TimestampMixin, Base):
    __tablename__ = "menus"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    description: Mapped[Optional[str]] = mapped_column(Text)
    price: Mapped[int] = mapped_column(Integer, nullable=False)
    lead_time_hours: Mapped[int] = mapped_column(Integer, default=48, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    orders: Mapped[list[Order]] = relationship(back_populates="menu")


class Astrologer(TimestampMixin, Base):
    __tablename__ = "astrologers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    display_name: Mapped[str] = mapped_column(String(100), nullable=False)
    login_email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    is_temp_password: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="active", nullable=False)
    commission_rate: Mapped[float] = mapped_column(Numeric(5, 2), default=60.00, nullable=False)
    stripe_account_id: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    stripe_onboarding_completed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    stripe_charges_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    stripe_payouts_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    line_accepting_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    line_accepting_status: Mapped[str] = mapped_column(String(20), default="open", nullable=False)
    line_accepting_message: Mapped[Optional[str]] = mapped_column(Text)

    orders: Mapped[list[Order]] = relationship(back_populates="assigned_reader")
    deliveries: Mapped[list[OrderDelivery]] = relationship(back_populates="reader")
    payouts: Mapped[list[Payout]] = relationship(back_populates="reader")
    yaml_logs: Mapped[list[YamlLog]] = relationship(back_populates="creator")


class AdminUser(TimestampMixin, Base):
    __tablename__ = "admin_users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    login_email: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    display_name: Mapped[str] = mapped_column(String(100), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    is_temp_password: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)


class AppSetting(TimestampMixin, Base):
    __tablename__ = "app_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(String(100), nullable=False, unique=True, index=True)
    value: Mapped[str] = mapped_column(Text, nullable=False, default="")


class LineSession(Base):
    __tablename__ = "line_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    line_user_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    state: Mapped[str] = mapped_column(String(50), nullable=False, default="idle")
    session_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class LineWebhookEvent(Base):
    __tablename__ = "line_webhook_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    webhook_event_id: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    line_user_id: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    event_type: Mapped[Optional[str]] = mapped_column(String(50))
    raw_event_json: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class DailyCardDraw(Base):
    __tablename__ = "daily_card_draws"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    line_user_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    display_name: Mapped[Optional[str]] = mapped_column(String(100))
    draw_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    card_id: Mapped[str] = mapped_column(String(100), nullable=False)
    drawn_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)




class IntakeDraft(TimestampMixin, Base):
    __tablename__ = "intake_drafts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    draft_code: Mapped[str] = mapped_column(String(32), unique=True, nullable=False, index=True)

    source: Mapped[str] = mapped_column(String(32), default="web", nullable=False, index=True)
    external_platform: Mapped[Optional[str]] = mapped_column(String(50))
    external_order_ref: Mapped[Optional[str]] = mapped_column(String(255), index=True)

    requested_menu_code: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    menu_id: Mapped[Optional[int]] = mapped_column(ForeignKey("menus.id"))
    order_kind: Mapped[str] = mapped_column(String(20), default="paid", nullable=False, index=True)

    user_name: Mapped[Optional[str]] = mapped_column(String(100))
    user_contact: Mapped[Optional[str]] = mapped_column(String(255), index=True)

    birth_date: Mapped[Optional[date]] = mapped_column(Date)
    birth_time: Mapped[Optional[str]] = mapped_column(String(20))
    birth_prefecture: Mapped[Optional[str]] = mapped_column(String(50))
    birth_place: Mapped[Optional[str]] = mapped_column(String(255))
    birth_lat: Mapped[Optional[float]] = mapped_column(Float)
    birth_lon: Mapped[Optional[float]] = mapped_column(Float)
    location_source: Mapped[Optional[str]] = mapped_column(String(50))
    location_note: Mapped[Optional[str]] = mapped_column(String(255))
    gender: Mapped[Optional[str]] = mapped_column(String(20))
    consultation_text: Mapped[Optional[str]] = mapped_column(Text)

    generate_ai: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    yaml_only: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    draft_status: Mapped[str] = mapped_column(String(30), default="input_pending", nullable=False, index=True)
    yaml_status: Mapped[str] = mapped_column(String(20), default="pending", nullable=False, index=True)
    ai_status: Mapped[str] = mapped_column(String(20), default="not_requested", nullable=False, index=True)

    latest_report_id: Mapped[Optional[int]] = mapped_column(ForeignKey("reports.id"))
    promoted_order_id: Mapped[Optional[int]] = mapped_column(ForeignKey("orders.id"), index=True)

    staff_memo: Mapped[Optional[str]] = mapped_column(Text)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime, index=True)

    menu: Mapped[Optional[Menu]] = relationship()
    latest_report: Mapped[Optional["Report"]] = relationship(foreign_keys=[latest_report_id])
    reports: Mapped[list["Report"]] = relationship(back_populates="draft", foreign_keys="Report.draft_id")


class Order(TimestampMixin, Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_code: Mapped[str] = mapped_column(String(32), unique=True, nullable=False, index=True)
    customer_id: Mapped[Optional[int]] = mapped_column(ForeignKey("customers.id"))
    source: Mapped[str] = mapped_column(String(32), default="self", nullable=False, index=True)
    order_kind: Mapped[str] = mapped_column(String(20), default="paid", nullable=False, index=True)
    free_reading_code: Mapped[Optional[str]] = mapped_column(String(32), unique=True, index=True)
    source_free_order_id: Mapped[Optional[int]] = mapped_column(ForeignKey("orders.id"))
    ai_status: Mapped[Optional[str]] = mapped_column(String(20), default="queued", index=True)
    external_platform: Mapped[Optional[str]] = mapped_column(String(50))
    external_order_ref: Mapped[Optional[str]] = mapped_column(String(255))
    user_name: Mapped[str] = mapped_column(String(100), nullable=False)
    user_contact: Mapped[Optional[str]] = mapped_column(String(255))
    birth_date: Mapped[date] = mapped_column(Date, nullable=False)
    birth_time: Mapped[Optional[str]] = mapped_column(String(20))
    birth_prefecture: Mapped[Optional[str]] = mapped_column(String(50))
    birth_place: Mapped[Optional[str]] = mapped_column(String(255))
    birth_lat: Mapped[Optional[float]] = mapped_column(Float)
    birth_lon: Mapped[Optional[float]] = mapped_column(Float)
    location_source: Mapped[Optional[str]] = mapped_column(String(50))
    location_note: Mapped[Optional[str]] = mapped_column(String(255))
    gender: Mapped[Optional[str]] = mapped_column(String(20))
    consultation_text: Mapped[Optional[str]] = mapped_column(Text)
    staff_memo: Mapped[Optional[str]] = mapped_column(Text)
    menu_id: Mapped[int] = mapped_column(ForeignKey("menus.id"), nullable=False)
    price: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="pending_payment", nullable=False, index=True)
    input_origin: Mapped[Optional[str]] = mapped_column(String(30), index=True)
    result_payload_json: Mapped[Optional[str]] = mapped_column(Text)
    result_html: Mapped[Optional[str]] = mapped_column(Text)
    free_result_text: Mapped[Optional[str]] = mapped_column(Text)
    assigned_reader_id: Mapped[Optional[int]] = mapped_column(ForeignKey("astrologers.id"))
    stripe_checkout_session_id: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    stripe_payment_intent_id: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    paid_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    delivered_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    primary_report_id: Mapped[Optional[int]] = mapped_column(ForeignKey("reports.id"))

    customer: Mapped[Optional[Customer]] = relationship(back_populates="orders")
    source_free_order: Mapped[Optional[Order]] = relationship(
        "Order",
        remote_side="Order.id",
        foreign_keys=[source_free_order_id],
        back_populates="derived_paid_orders",
    )
    derived_paid_orders: Mapped[list[Order]] = relationship(
        "Order",
        foreign_keys=[source_free_order_id],
        back_populates="source_free_order",
    )
    menu: Mapped[Menu] = relationship(back_populates="orders")
    assigned_reader: Mapped[Optional[Astrologer]] = relationship(back_populates="orders")
    deliveries: Mapped[list[OrderDelivery]] = relationship(back_populates="order", cascade="all, delete-orphan")
    payouts: Mapped[list[Payout]] = relationship(back_populates="order", cascade="all, delete-orphan")
    status_logs: Mapped[list[OrderStatusLog]] = relationship(back_populates="order", cascade="all, delete-orphan")
    payment_transactions: Mapped[list[PaymentTransaction]] = relationship(back_populates="order", cascade="all, delete-orphan")
    yaml_logs: Mapped[list[YamlLog]] = relationship(back_populates="order", cascade="all, delete-orphan")
    input_snapshots: Mapped[list[OrderInputSnapshot]] = relationship(back_populates="order", cascade="all, delete-orphan")
    result_views: Mapped[list[OrderResultView]] = relationship(back_populates="order", cascade="all, delete-orphan")
    primary_report: Mapped[Optional["Report"]] = relationship(foreign_keys=[primary_report_id])
    reports: Mapped[list["Report"]] = relationship(back_populates="order", foreign_keys="Report.order_id")


class OrderDelivery(TimestampMixin, Base):
    __tablename__ = "order_deliveries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), nullable=False)
    reader_id: Mapped[int] = mapped_column(ForeignKey("astrologers.id"), nullable=False)
    delivery_text: Mapped[str] = mapped_column(Text, default="", nullable=False)
    is_draft: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    delivered_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    order: Mapped[Order] = relationship(back_populates="deliveries")
    reader: Mapped[Astrologer] = relationship(back_populates="deliveries")


class Payout(TimestampMixin, Base):
    __tablename__ = "payouts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), nullable=False)
    reader_id: Mapped[int] = mapped_column(ForeignKey("astrologers.id"), nullable=False)
    gross_amount: Mapped[int] = mapped_column(Integer, nullable=False)
    commission_rate: Mapped[float] = mapped_column(Numeric(5, 2), nullable=False)
    reader_amount: Mapped[int] = mapped_column(Integer, nullable=False)
    platform_amount: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="unpaid", nullable=False)
    scheduled_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    paid_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    order: Mapped[Order] = relationship(back_populates="payouts")
    reader: Mapped[Astrologer] = relationship(back_populates="payouts")


class OrderStatusLog(Base):
    __tablename__ = "order_status_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), nullable=False)
    from_status: Mapped[Optional[str]] = mapped_column(String(30))
    to_status: Mapped[str] = mapped_column(String(30), nullable=False)
    actor_type: Mapped[str] = mapped_column(String(20), nullable=False)
    actor_id: Mapped[Optional[int]] = mapped_column(Integer)
    note: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    order: Mapped[Order] = relationship(back_populates="status_logs")


class PaymentTransaction(TimestampMixin, Base):
    __tablename__ = "payment_transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), nullable=False)
    provider: Mapped[str] = mapped_column(String(30), nullable=False, default="stripe")
    provider_payment_id: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    provider_session_id: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    amount: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(String(10), nullable=False, default="jpy")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    paid_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    raw_event_json: Mapped[Optional[str]] = mapped_column(Text)

    order: Mapped[Order] = relationship(back_populates="payment_transactions")


class OrderInputSnapshot(Base):
    __tablename__ = "order_input_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), nullable=False)
    inputs_json: Mapped[Optional[str]] = mapped_column(Text)
    payload_json: Mapped[Optional[str]] = mapped_column(Text)
    unknowns_json: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    order: Mapped[Order] = relationship(back_populates="input_snapshots")




class Report(TimestampMixin, Base):
    __tablename__ = "reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    draft_id: Mapped[Optional[int]] = mapped_column(ForeignKey("intake_drafts.id"), index=True)
    order_id: Mapped[Optional[int]] = mapped_column(ForeignKey("orders.id"), index=True)

    report_type: Mapped[str] = mapped_column(String(50), nullable=False, index=True)

    yaml_status: Mapped[str] = mapped_column(String(20), default="pending", nullable=False, index=True)
    yaml_payload: Mapped[Optional[str]] = mapped_column(Text)

    ai_status: Mapped[str] = mapped_column(String(20), default="not_requested", nullable=False, index=True)
    sections_json: Mapped[Optional[str]] = mapped_column(Text)
    result_payload_json: Mapped[Optional[str]] = mapped_column(Text)
    result_html: Mapped[Optional[str]] = mapped_column(Text)

    model: Mapped[Optional[str]] = mapped_column(String(100))
    prompt_version: Mapped[Optional[str]] = mapped_column(String(50))
    generated_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    error_message: Mapped[Optional[str]] = mapped_column(Text)

    draft: Mapped[Optional["IntakeDraft"]] = relationship(back_populates="reports", foreign_keys=[draft_id])
    order: Mapped[Optional[Order]] = relationship(back_populates="reports", foreign_keys=[order_id])


class YamlLog(TimestampMixin, Base):
    __tablename__ = "yaml_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), nullable=False)
    customer_id: Mapped[Optional[int]] = mapped_column(ForeignKey("customers.id"))
    yaml_body: Mapped[str] = mapped_column(Text, nullable=False)
    summary_json: Mapped[Optional[str]] = mapped_column(Text)
    created_by_type: Mapped[str] = mapped_column(String(20), nullable=False, default="system")
    created_by_id: Mapped[Optional[int]] = mapped_column(ForeignKey("astrologers.id"))
    log_type: Mapped[str] = mapped_column(String(30), default="generated", nullable=False)
    version_no: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    order: Mapped[Order] = relationship(back_populates="yaml_logs")
    customer: Mapped[Optional[Customer]] = relationship(back_populates="yaml_logs")
    creator: Mapped[Optional[Astrologer]] = relationship(back_populates="yaml_logs")


class OrderResultView(TimestampMixin, Base):
    __tablename__ = "order_result_views"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), nullable=False, index=True)
    source_yaml_log_id: Mapped[Optional[int]] = mapped_column(ForeignKey("yaml_logs.id"))
    result_payload_json: Mapped[Optional[str]] = mapped_column(Text)
    result_html: Mapped[Optional[str]] = mapped_column(Text)
    horoscope_image_url: Mapped[Optional[str]] = mapped_column(String(500))
    published_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    updated_by_type: Mapped[Optional[str]] = mapped_column(String(20))
    updated_by_id: Mapped[Optional[int]] = mapped_column(Integer)
    report_html: Mapped[Optional[str]] = mapped_column(Text)
    report_generated_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    order: Mapped[Order] = relationship(back_populates="result_views")
    source_yaml_log: Mapped[Optional[YamlLog]] = relationship()


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    actor_type: Mapped[str] = mapped_column(String(20), nullable=False)
    actor_id: Mapped[Optional[int]] = mapped_column(Integer)
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    target_type: Mapped[str] = mapped_column(String(50), nullable=False)
    target_id: Mapped[Optional[int]] = mapped_column(Integer)
    before_json: Mapped[Optional[str]] = mapped_column(Text)
    after_json: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)


class ExternalOrder(TimestampMixin, Base):
    __tablename__ = "external_orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_code: Mapped[str] = mapped_column(String(32), unique=True, nullable=False, index=True)
    source_type: Mapped[str] = mapped_column(String(20), nullable=False, default="coconala", index=True)
    customer_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    customer_email: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    birth_date: Mapped[Optional[date]] = mapped_column(Date)
    birth_time: Mapped[Optional[str]] = mapped_column(String(10))
    gender: Mapped[Optional[str]] = mapped_column(String(20))
    prefecture: Mapped[Optional[str]] = mapped_column(String(100))
    birth_place: Mapped[Optional[str]] = mapped_column(String(255))
    consultation_text: Mapped[Optional[str]] = mapped_column(Text)
    menu_name: Mapped[Optional[str]] = mapped_column(String(255))
    price: Mapped[Optional[int]] = mapped_column(Integer)
    staff_name: Mapped[Optional[str]] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="draft", index=True)
    yaml_log_text: Mapped[Optional[str]] = mapped_column(Text)
    html_storage_path: Mapped[Optional[str]] = mapped_column(Text)
    html_original_name: Mapped[Optional[str]] = mapped_column(String(255))
    html_uploaded_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    public_token: Mapped[Optional[str]] = mapped_column(String(128), unique=True, index=True)
    public_url: Mapped[Optional[str]] = mapped_column(Text)
    url_issued_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime, index=True)
    mail_subject: Mapped[Optional[str]] = mapped_column(Text)
    mail_body: Mapped[Optional[str]] = mapped_column(Text)
    mail_sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    delivered_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # 共通鑑定書生成エンジン用（外部受注 → STORES連携にも拡張しやすい形）
    report_generation_status: Mapped[Optional[str]] = mapped_column(String(30))
    report_generation_plan: Mapped[Optional[str]] = mapped_column(String(30))
    report_generation_system: Mapped[Optional[str]] = mapped_column(String(50))
    report_generation_prompt_key: Mapped[Optional[str]] = mapped_column(String(100))
    report_generation_model: Mapped[Optional[str]] = mapped_column(String(100))
    report_generated_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    last_error: Mapped[Optional[str]] = mapped_column(Text)


class TransitHubRequest(TimestampMixin, Base):
    __tablename__ = "transit_hub_requests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    request_code: Mapped[str] = mapped_column(String(32), unique=True, nullable=False, index=True)
    channel: Mapped[str] = mapped_column(String(30), nullable=False, default="manual", index=True)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="draft", index=True)

    customer_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    customer_email: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    birth_date: Mapped[Optional[date]] = mapped_column(Date)
    birth_time: Mapped[Optional[str]] = mapped_column(String(20))
    gender: Mapped[Optional[str]] = mapped_column(String(20))
    prefecture: Mapped[Optional[str]] = mapped_column(String(100))
    birth_place: Mapped[Optional[str]] = mapped_column(String(255))

    period_label: Mapped[str] = mapped_column(String(50), nullable=False, default="3ヶ月")
    period_start: Mapped[Optional[date]] = mapped_column(Date, index=True)
    period_end: Mapped[Optional[date]] = mapped_column(Date, index=True)
    template_name: Mapped[str] = mapped_column(String(100), nullable=False, default="standard_3month")

    notes: Mapped[Optional[str]] = mapped_column(Text)
    generated_summary: Mapped[Optional[str]] = mapped_column(Text)
    generated_html: Mapped[Optional[str]] = mapped_column(Text)
    generated_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    last_error: Mapped[Optional[str]] = mapped_column(Text)

    jobs: Mapped[list["TransitHubJob"]] = relationship(back_populates="request", cascade="all, delete-orphan")


class TransitHubJob(TimestampMixin, Base):
    __tablename__ = "transit_hub_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    request_id: Mapped[int] = mapped_column(ForeignKey("transit_hub_requests.id"), nullable=False, index=True)
    job_type: Mapped[str] = mapped_column(String(30), nullable=False, default="generate")
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", index=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    log_text: Mapped[Optional[str]] = mapped_column(Text)

    request: Mapped[TransitHubRequest] = relationship(back_populates="jobs")
