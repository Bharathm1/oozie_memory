import random
import matplotlib.pyplot as plt

def generate_numbers():
    choices = [+200, -200, +10, +800, -800, +40, +400]
    numbers = [random.choice(choices) for _ in range(100)]
    return numbers

def plot_cumulative_sum(numbers):
    cumulative_sum = [sum(numbers[:i+1]) for i in range(len(numbers))]
    plt.plot(cumulative_sum, marker='o', linestyle='-')
    plt.xlabel('Number of Additions')
    plt.ylabel('Cumulative Sum')
    plt.title('Cumulative Sum Graph')
    plt.grid()
    plt.show()

def main():
    numbers = generate_numbers()
    total_sum = sum(numbers)
    print("Generated numbers:", numbers)
    print("Total sum:", total_sum)
    plot_cumulative_sum(numbers)

if __name__ == "__main__":
    main()
