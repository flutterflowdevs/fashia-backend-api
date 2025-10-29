def paginate(query, page: int, page_size: int):
    total_items = query.count()
    total_pages = (total_items + page_size - 1) // page_size
    items = query.offset((page - 1) * page_size).limit(page_size).all()
    
    return {
        "total_items": total_items,
        "total_pages": total_pages,
        "current_page": page,
        "page_size": page_size,
        "items": items,
    }