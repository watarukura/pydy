from src.fizz_buzz_plus import FizzBuzzPlus


def test_fizz_buzz():
    assert "Fizz" in FizzBuzzPlus().answer(3)
    assert "Fizz" in FizzBuzzPlus().answer(9)
    assert "Fizz" in FizzBuzzPlus().answer(31)
    assert "Fizz" in FizzBuzzPlus().answer(38)
    assert "Buzz" in FizzBuzzPlus().answer(10)
    assert "Buzz" in FizzBuzzPlus().answer(51)
    assert "Buzz" in FizzBuzzPlus().answer(58)
    assert "7" == FizzBuzzPlus().answer(7)
