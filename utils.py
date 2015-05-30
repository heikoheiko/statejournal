
def pareto(x, alpha=.1, Xm=1.):
    x += 1
    assert alpha > 0
    assert Xm > 0
    if x < Xm:
        return 0
    alpha = float(alpha)
    return alpha * Xm ** alpha / x**(alpha+1)

upper_x = 0.01

def get_pareto(alpha, max_v, max_x=1000):
    norm = max_v / pareto(0., alpha)
    def _pareto(x):
        x = x % max_x
        x *= upper_x / max_x
        return pareto(x, alpha) * norm
    return _pareto


def get_alpha(lower_x=0.20, cumulate_for=.80):
    num_samples = 10000
    i_norm = upper_x / num_samples
    alpha = 0.1
    while True:
        vals = [pareto(i * i_norm, alpha) for i in range(0, num_samples)]
        cum_fraction = sum(vals[:int(lower_x * num_samples)]) / float(sum(vals))
        if cum_fraction > cumulate_for:
            break
        alpha *= 1.01
    return alpha




if __name__ == '__main__':
    max_x = 1000
    max_v = 156
    lower_x = 0.4
    cumulate_for = .6
    alpha = get_alpha(lower_x, cumulate_for)
    print alpha, max_v, max_x
    P = get_pareto(alpha, max_v=max_v, max_x=max_x)
    for i in range(0, max_x, max_x/10):
        print i, P(i)
    print max_x, P(max_x)

