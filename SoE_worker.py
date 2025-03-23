#!/bin/bash
import sys
import time

def sieve_of_eratosthenes(start, end):
    primes = []
    sieve = [True] * (end + 1)
    for num in range(2, end + 1):
        if sieve[num]:
            if num >= start:
                primes.append(num)
            for multiple in range(num * num, end + 1, num):
                sieve[multiple] = False
        if num >= start: time.sleep(0.005)  # Simulate workload
    return primes

if __name__ == "__main__":
    start, end = map(int, sys.argv[1:3])
    primes = sieve_of_eratosthenes(start, end)
    print(f"Primes in range {start}-{end}: {primes}")
