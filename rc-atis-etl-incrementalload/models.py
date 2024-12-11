from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy import String, Integer, Float


class Base(DeclarativeBase):
    pass


class RaliwayCrossingData(Base):

    __tablename__ = 'RailwayCrossingData'

    event_id = mapped_column(Integer, primary_key=True)
    event_key = mapped_column(String(50))
    created = mapped_column(Float)
    time_zone_bias = mapped_column(Float)
    server = mapped_column(String(50))
    category = mapped_column(String(50))
    description = mapped_column(String(2000))
    color = mapped_column(Integer)
    priority = mapped_column(Integer)
    ack_date = mapped_column(Float)
    ack_user = mapped_column(String(50))
    ack_comment = mapped_column(String(2000))
    script_tag = mapped_column(String(255))
    meta_data = mapped_column(String)
    url = mapped_column(String(500))
    remote_url = mapped_column(String(500))
    latitude = mapped_column(Float)
    longitude = mapped_column(Float)
    associated_devices = mapped_column(String(2000))
    ack_interval = mapped_column(Integer)
    owner_interval = mapped_column(Integer)
    owner = mapped_column(String(50))
    last_owner = mapped_column(String(50))
    last_owner_date = mapped_column(Float)


class RailwayCrossingDataRP(Base):

    __tablename__ = 'RailwayCrossingDataRP'

    rp_id = mapped_column(Integer, primary_key=True)
    name = mapped_column(String(255))
    prop_value = mapped_column(String(255))
    update_time = mapped_column(Float)
    initial_value = mapped_column(Integer)
    trend_id = mapped_column(Integer)
    server = mapped_column(String(50))
    read_prop_update_id = mapped_column(Integer)