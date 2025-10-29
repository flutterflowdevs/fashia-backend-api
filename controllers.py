from fastapi import APIRouter, HTTPException
from supabase_client import supabase_client
from typing import List, Optional

router = APIRouter()

# Product Read Operations
@router.get("/products", tags=["products"])
async def read_products(limit: int = 100):
    """
    Retrieve all products
    """
    try:
        products = supabase_client.get_products(limit=limit)
        return {"success": True, "data": products, "count": len(products)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/products/{product_id}", tags=["products"])
async def read_product(product_id: int):
    """
    Retrieve a single product by ID
    """
    try:
        product = supabase_client.get_product_by_id(product_id)
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        return {"success": True, "data": product}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# User Read Operations
@router.get("/users", tags=["users"])
async def read_users(limit: int = 100):
    """
    Retrieve all users
    """
    try:
        users = supabase_client.get_users(limit=limit)
        return {"success": True, "data": users, "count": len(users)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/users/{user_id}", tags=["users"])
async def read_user(user_id: int):
    """
    Retrieve a single user by ID
    """
    try:
        user = supabase_client.get_user_by_id(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return {"success": True, "data": user}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Order Read Operations
@router.get("/orders", tags=["orders"])
async def read_orders(limit: int = 100):
    """
    Retrieve all orders
    """
    try:
        orders = supabase_client.get_orders(limit=limit)
        return {"success": True, "data": orders, "count": len(orders)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/orders/{order_id}", tags=["orders"])
async def read_order(order_id: int):
    """
    Retrieve a single order by ID
    """
    try:
        order = supabase_client.get_order_by_id(order_id)
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        return {"success": True, "data": order}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
