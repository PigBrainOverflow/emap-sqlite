def rewrite_tags(**kwargs):
    def decorator(func):
        func._rewrite_tags = kwargs
        return func
    return decorator