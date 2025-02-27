import os
from sqlalchemy import create_engine, Column, String, Float, Date, Integer, PrimaryKeyConstraint
from sqlalchemy.orm import sessionmaker, declarative_base
from fetch_fuel_data import fetch_fuel_prices  # Importing function from fuel script
from fetch_electricity_rates import fetch_electricity_rates  # Importing function from electricity script

# Load environment variables
DB_CONN = os.getenv("DATABASE_URL")

# Set up SQLAlchemy
Base = declarative_base()
engine = create_engine(DB_CONN)
Session = sessionmaker(bind=engine)

# Define Fuel Prices Table
class FuelPrice(Base):
    __tablename__ = "fuel_prices"
    state_id = Column(String, primary_key=True)
    fuel_type = Column(String, primary_key=True)
    price_date = Column(Date, primary_key=True)
    avg_price = Column(Float, nullable=False)

# Define Electricity Rates Table
class ElectricityRate(Base):
    __tablename__ = "electricity_rates"
    state_id = Column(String, primary_key=True)
    rate_date = Column(Date, primary_key=True)
    avg_rate_kwh = Column(Float, nullable=False)

# Create Tables if They Do Not Exist
def create_tables():
    Base.metadata.create_all(engine)
    print("Database tables created successfully.")

# Insert or Update Fuel Prices
def insert_fuel_prices():
    session = Session()
    data = fetch_fuel_prices()
    
    for entry in data:
        obj = session.query(FuelPrice).filter_by(
            state_id=entry["state_id"],
            fuel_type=entry["fuel_type"],
            price_date=entry["price_date"]
        ).first()
        
        if obj:
            obj.avg_price = entry["avg_price"]  # Update existing record
        else:
            obj = FuelPrice(**entry)
            session.add(obj)  # Insert new record
    
    session.commit()
    session.close()
    print("Fuel prices inserted/updated successfully.")

# Insert or Update Electricity Rates
def insert_electricity_rates():
    session = Session()
    data = fetch_electricity_rates()
    
    for entry in data:
        obj = session.query(ElectricityRate).filter_by(
            state_id=entry["state_id"],
            rate_date=entry["rate_date"]
        ).first()
        
        if obj:
            obj.avg_rate_kwh = entry["avg_rate_kwh"]  # Update existing record
        else:
            obj = ElectricityRate(**entry)
            session.add(obj)  # Insert new record
    
    session.commit()
    session.close()
    print("Electricity rates inserted/updated successfully.")

if __name__ == "__main__":
    create_tables()
    insert_fuel_prices()
    insert_electricity_rates()
