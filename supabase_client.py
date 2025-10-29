from supabase import create_client, Client
from app.core.config import settings

class SupabaseClient:
    def __init__(self):
        self.url: str = settings.SUPABASE_URL
        self.key: str = settings.SUPABASE_KEY
        self.client: Client = create_client(self.url, self.key)
    
    def get_client(self) -> Client:
        return self.client
    
    # Products operations
    def get_products(self, limit: int = 100):
        response = self.client.table('products').select('*').limit(limit).execute()
        return response.data
    
    def get_product_by_id(self, product_id: int):
        response = self.client.table('products').select('*').eq('id', product_id).execute()
        return response.data
    
    # Users operations
    def get_users(self, limit: int = 100):
        response = self.client.table('users').select('*').limit(limit).execute()
        return response.data
    
    def get_user_by_id(self, user_id: int):
        response = self.client.table('users').select('*').eq('id', user_id).execute()
        return response.data
    
    # Orders operations
    def get_orders(self, limit: int = 100):
        response = self.client.table('orders').select('*').limit(limit).execute()
        return response.data
    
    def get_order_by_id(self, order_id: int):
        response = self.client.table('orders').select('*').eq('id', order_id).execute()
        return response.data

supabase_client = SupabaseClient()
