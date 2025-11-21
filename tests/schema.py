import enum

from sqlalchemy_schema_factory import factory  # type: ignore[import-not-found]


class DeviceGroupKind(str, enum.Enum):
    devices = "devices"
    users = "users"


HARDWARE_PLATFORMS = "hardware_platforms"
GROUPS = "groups"
HARDWARE_ITEMS = "hardware"
DEVICES = "devices"
EVENTS = "events"
GROUPS_DEVICES = "groups_devices"
TAGS = "tags"
FIRMWARE_ITEMS = "firmware"
FIRMWARE_TAGS = "firmware_tags"
HARDWARE_FIRMWARE = "hardware_firmware"

db_metadata = factory.metadata()

event = factory.actions_tracked_table(
    name=EVENTS,
    db_metadata=db_metadata,
    columns=(
        factory.uuid_primary_key(),
        factory.string(name="name", nullable=False),
        factory.jsonb(name="payload", nullable=True),
    ),
)

group = factory.actions_tracked_table(
    name="",
    db_metadata=db_metadata,
    columns=(
        factory.uuid_primary_key(),
        factory.uuid(name="owner_id", nullable=False),
        factory.string(name="name", nullable=False),
        factory.enum(name="kind", type_=DeviceGroupKind, nullable=False),
    ),
)

hardware_platform = factory.actions_tracked_table(
    name=HARDWARE_PLATFORMS,
    db_metadata=db_metadata,
    columns=(
        factory.uuid_primary_key(),
        factory.string(name="name", nullable=False),
        factory.string(name="code", nullable=False),
    ),
)

hardware = factory.table(
    name=HARDWARE_ITEMS,
    db_metadata=db_metadata,
    columns=(
        factory.uuid_primary_key(),
        factory.string(name="name", nullable=False),
        factory.string(name="revision", nullable=False),
        factory.foreign_key(to_=hardware_platform, on_=hardware_platform.c.id, name="platform_id"),
    ),
)

device = factory.actions_tracked_table(
    name=DEVICES,
    db_metadata=db_metadata,
    columns=(
        factory.uuid_primary_key(),
        factory.uuid(name="owner_id", nullable=False),
        factory.string(name="name", nullable=False),
        factory.string(name="description"),
        factory.string(name="serial", nullable=False),
        factory.foreign_key(to_=hardware, name="hardware_id"),
    ),
)

group_device = factory.actions_tracked_table(
    name=GROUPS_DEVICES,
    db_metadata=db_metadata,
    columns=(
        factory.uuid_primary_key(),
        factory.foreign_key(to_=device, on_=device.c.id, name="device_id"),
        factory.foreign_key(to_=group, on_=group.c.id, name="group_id"),
    ),
)

tag = factory.actions_tracked_table(
    name=TAGS,
    db_metadata=db_metadata,
    columns=(
        factory.uuid_primary_key(),
        factory.string(name="name", nullable=False),
    ),
)

firmware = factory.actions_tracked_table(
    name=FIRMWARE_ITEMS,
    db_metadata=db_metadata,
    columns=(
        factory.uuid_primary_key(),
        factory.string(name="uri", nullable=False),
        factory.string(name="version", nullable=False),
        factory.jsonb(name="payload", nullable=False),
        factory.foreign_key(to_=tag, on_=tag.c.id, name="tag_id"),
    ),
)

hardware_firmware = factory.actions_tracked_table(
    name=HARDWARE_FIRMWARE,
    db_metadata=db_metadata,
    columns=(
        factory.uuid_primary_key(),
        factory.foreign_key(to_=hardware, on_=hardware.c.id, name="hardware_id"),
        factory.foreign_key(to_=firmware, on_=firmware.c.id, name="firmware_id"),
    ),
)
