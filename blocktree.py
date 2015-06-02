class Block(object):

    def __init__(self, number, parent=None):
        self.number = number
        self.parent = parent

    def __repr__(self):
        return str(self.number)

    def hashparents(self):
        hp = [self.parent.number] if self.parent else []
        if self.number < 2:
            return hp
        p = 0
        while self.number % (2 ** p) == 0:
            p += 1

        p -= 1
        bn = self.number - 2**p
        # assert bn < self.number
        b = self.get_block(bn)
        # hp.extend(b.hashparents())
        if bn not in hp:
            hp.append(bn)
        return hp

    def get_block(self, number):
        assert 0 <= number <= self.number
        if self.number == number:
            return self
        assert self.parent
        return self.parent.get_block(number)

if __name__ == '__main__':
    b = Block(0, parent=None)
    for i in range(257):
        hp = b.hashparents()
        print b.number, hp
        b = Block(number=b.number + 1, parent=b)
