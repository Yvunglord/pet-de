from datetime import datetime
from sqlalchemy import Column, Integer, String, Numeric, Boolean, DateTime, ForeignKey, Index
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class RawTrade(Base):
    __tablename__ = 'raw_trades'
    
    trade_id = Column(String, primary_key=True)
    price = Column(Numeric, nullable=False)
    quantity = Column(Numeric, nullable=False)
    trade_time = Column(DateTime(timezone=True), nullable=False)
    is_buyer_maker = Column(Boolean, nullable=False)
    loaded_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_raw_trades_time', trade_time.desc()),
    )
    
    def __repr__(self):
        return f"<RawTrade(trade_id={self.trade_id}, price={self.price})>"
    
class Category(Base):
    __tablename__ = 'categories'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)
    created_at = Column(DateTime(timezone=True), default=datetime.now)
    
    products = relationship("Product", back_populates="category_ref")
    
    def __repr__(self):
        return f"<Category(name={self.name})>"


class Product(Base):
    __tablename__ = 'products'
    
    product_id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    price = Column(Numeric, nullable=False)
    category_id = Column(Integer, ForeignKey('categories.id', ondelete='SET NULL'), nullable=True)
    category_name = Column(String)
    updated_at = Column(DateTime(timezone=True), default=datetime.now, onupdate=datetime.now)
    
    category_ref = relationship("Category", back_populates="products")
    
    __table_args__ = (
        Index('idx_products_category', category_id),
    )
    
    def __repr__(self):
        return f"<Product(product_id={self.product_id}, title={self.title})>"