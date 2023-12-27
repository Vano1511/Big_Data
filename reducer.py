def reducer(price_1, price_2):
    n, mean, var = 0, 0.0, 0.0
    if price_1:
        if isinstance(price_1, tuple):
            n, mean, var = price_1
        else:
            n += 1
            mean = price_1
    if price_2:
        n += 1
        score = price_2
        delta = score - mean
        mean += delta / n
        var += delta * (score - mean)
    return n, mean, var
