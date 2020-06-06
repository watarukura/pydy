class FizzBuzzPlus:
    @staticmethod
    def answer(number: int) -> str:
        """Fizz or Buzzを判定する
        Args:
            number: integer input
        Returns:
            str: Fizz or Buzz or Number as string
        """
        text = ""
        if number % 3 == 0:
            text += "Fizz"
        if number % 5 == 0:
            text += "Buzz"
        if "3" in str(number):
            text += "Fizz"
        if "5" in str(number):
            text += "Buzz"
        if text == "":
            text = str(number)
        return text


def main():
    for i in range(1, 101):
        print(FizzBuzzPlus().answer(i))


if __name__ == "__main__":
    main()
