import operator

ops = {
    ">": operator.gt,
    "<": operator.lt,
    "==": operator.eq,
    "!=": operator.ne,
    ">=": operator.ge,
    "<=": operator.le,
    "contains": lambda a, b: b in a
}

def evaluate_rule(transaction_dict, rule):
    """Kiểm tra một transaction có vi phạm rule hay không"""
    if not rule.get("enabled"):
        return False
    
    params = rule.get("params", {})
    field = params.get("field")
    op_str = params.get("op")
    threshold = params.get("value")
    
    actual_value = transaction_dict.get(field)
    
    if actual_value is None:
        return False
    
    # Thực hiện so sánh: actual_value > threshold
    try:
        return ops[op_str](float(actual_value), float(threshold))
    except:
        return False