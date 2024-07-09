class NotEnoughLiquidityException(Exception):
    """
    Thrown when there is not enough liquidity to complete the given swap.
    """
    amount_in: int
    remaining: int

    def __init__(self, amount_in, remaining, *args: object) -> None:
        super().__init__(*args)
        self.amount_in = amount_in
        self.remaining = remaining

    def __str__(self) -> str:
        return f'<NotEnoughLiquidityException amount_in={self.amount_in} amount_remaining={self.remaining}>'
