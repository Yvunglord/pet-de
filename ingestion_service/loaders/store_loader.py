from typing import Any, Dict

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..database.models import Category, Product
from ..logger import log


class StoreDataLoader:

    def __init__(self, session_factory):
        self.session_factory = session_factory

    async def load(self, product_data: Dict[str, Any]):
        try:
            async with self.session_factory() as session:
                category_id = await self._get_or_create_category(
                    session, product_data["category"]
                )

                stmt = pg_insert(Product).values(
                    product_id=product_data["product_id"],
                    title=product_data["title"],
                    price=product_data["price"],
                    category_id=category_id,
                    category_name=product_data["category"],
                    updated_at=product_data["updated_at"],
                )

                stmt = stmt.on_conflict_do_update(
                    index_elements=["product_id"],
                    set_=dict(
                        price=product_data["price"],
                        title=product_data["title"],
                        category_id=category_id,
                        category_name=product_data["category"],
                        updated_at=product_data["updated_at"],
                    ),
                )

                await session.execute(stmt)
                await session.commit()

            log.debug(f"Loaded product {product_data['product_id']} to store DB")

        except Exception as e:
            log.exception(
                f"Failed to load product {product_data.get('product_id')}: {e}"
            )

    async def _get_or_create_category(
        self, session: AsyncSession, category_name: str
    ) -> int:
        result = await session.execute(
            select(Category).where(Category.name == category_name)
        )
        category = result.scalar_one_or_none()

        if category:
            return category.id

        new_category = Category(name=category_name)
        session.add(new_category)
        await session.flush()

        return new_category.id
