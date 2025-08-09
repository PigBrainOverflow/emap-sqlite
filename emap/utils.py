from typing import Iterable


class RollingHash:
    _POWER_B: list[int]
    _M: int

    def __init__(self, B: int = 257, M: int = 10**9+7):
        self._POWER_B = [1, B]
        self._M = M

    def hash(self, xs: Iterable[int]) -> int:
        h = 0
        for x in xs:
            h = (h * self._POWER_B[1] + x) % self._M
        return h

    def update(self, old_h: int, index: int, old_x: int, new_x: int) -> int:
        """
        Update the hash value by replacing old_x at index with new_x.
        """
        while len(self._POWER_B) <= index:
            self._POWER_B.append((self._POWER_B[-1] * self._POWER_B[1]) % self._M)
        h = (old_h - old_x * self._POWER_B[index]) % self._M    # TODO: is it safe? underflow?
        h = (h + new_x * self._POWER_B[index]) % self._M
        return h