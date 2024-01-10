# Adaptive Filter

import random

MIN_SELECTIVITY = 0.1
MAX_SELECTIVITY = 1
MIN_ROW_COST = 1
MAX_ROW_COST = 5
MIN_FILTER_EXPR_NUM = 2
MAX_FILTER_EXPR_NUM = 10
BLOCK_SIZE = 65536
NUM_BLOCKS = 512

class Filter:
    def __init__(self):
        self.selectivity = random.uniform(MIN_SELECTIVITY, MAX_SELECTIVITY)
        self.row_cost = random.uniform(MIN_ROW_COST, MAX_ROW_COST)
    
    def filter(self, row_count):
        return row_count * self.selectivity
    
    def cost(self, row_count):
        return row_count * self.row_cost

class DatabendFilterPermutation:
    def __init__(self, num_filter_exprs):
        self.observe = False
        self.keep_better = False
        self.swap_idx = 0
        self.runtime_sum = 0
        self.runtime_count = 0
        self.swap_possibility = []
        self.permutation = []
        for i in range(num_filter_exprs):
            self.permutation.append(i)
            if i != num_filter_exprs - 1:
                self.swap_possibility.append(100)
        self.random_number_border = 100 * (num_filter_exprs - 1)

    def add_statistics(self, runtime):
        # Whether there is a swap to be observed.
        if self.observe:
            last_runtime = self.runtime_sum / self.runtime_count
            
            # Observe the last swap, if runtime decreased, keep swap, else reverse swap.
            if last_runtime <= runtime:
                # Reverse swap because runtime didn't decrease, we don't update runtime_sum and runtime_count 
                # and we will continue to use the old runtime_sum and runtime_count in next iteration.
                self.permutation[self.swap_idx], self.permutation[self.swap_idx + 1] = self.permutation[self.swap_idx + 1], self.permutation[self.swap_idx]
                # Decrease swap possibility, but make sure there is always a small possibility left.
                if self.swap_possibility[self.swap_idx] > 1:
                    self.swap_possibility[self.swap_idx] /= 2
            else:
                # Keep swap because runtime decreased, reset possibility.
                self.swap_possibility[self.swap_idx] = 100
                # Reset runtime_sum and runtime_count.
                self.runtime_sum = runtime
                self.runtime_count = 1
                # This is a better swap, we keep it at least once.
                self.keep_better = True
            
            self.observe = False
        else:
            random_number = random.randint(0, self.random_number_border - 1)
            # We will swap the filter expression at index swap_idx and swap_idx + 1.
            self.swap_idx = random_number // 100 
            possibility = random_number - 100 * self.swap_idx
            
            # Check if swap is going to happen.
            if not self.keep_better and self.swap_possibility[self.swap_idx] > possibility:
                # Swap.
                self.permutation[self.swap_idx], self.permutation[self.swap_idx + 1] = self.permutation[self.swap_idx + 1], self.permutation[self.swap_idx]
                # Observe whether this swap is a better swap.
                self.observe = True
            
            # Don't need to keep this permutation anymore.
            self.keep_better = False
            # Update runtime_sum and runtime_count.
            self.runtime_sum += runtime
            self.runtime_count += 1

class FilterExecutor:
    def __init__(self, filter_exprs):
        self.filter_exprs = filter_exprs
        self.databend_filter_permutation = DatabendFilterPermutation(len(filter_exprs))

    def cost(self, row_count, permutation):
        cost = 0
        for i in permutation:
            cost += self.filter_exprs[i].cost(row_count)
            row_count = self.filter_exprs[i].filter(row_count)
        return cost

    def filter(self, row_count):
        cost = self.cost(row_count, self.databend_filter_permutation.permutation)
        self.databend_filter_permutation.add_statistics(cost)
        return cost

class MinCostPermutation:
    def __init__(self, filter_exprs):
        self.filter_exprs = filter_exprs
        self.permutation = [0] * len(filter_exprs)
        self.visit = [False] * len(filter_exprs)
        self.min_cost = MAX_FILTER_EXPR_NUM * BLOCK_SIZE * MAX_ROW_COST

    def cost(self, filter_exprs, row_count):
        cost = 0
        for i in self.permutation:
            cost += filter_exprs[i].cost(row_count)
            row_count = filter_exprs[i].filter(row_count)
        return cost

    def permutation_min_cost(self, idx, len, filter_exprs, row_count):
        if idx == len:
            current_cost = self.cost(filter_exprs, row_count)
            self.min_cost = min(self.min_cost, current_cost)
            return
        for i in range(len):
            if not self.visit[i]:
                self.visit[i] = True
                self.permutation[idx] = i
                self.permutation_min_cost(idx + 1, len, filter_exprs, row_count)
                self.visit[i] = False

if __name__ == "__main__":
    original = [0] * (MAX_FILTER_EXPR_NUM + 1)
    adaptive = [0] * (MAX_FILTER_EXPR_NUM + 1)
    run_num = 100
    for i in range(run_num):
        print("[{}/{}]".format(i + 1, run_num))
        for num_filter_exprs in range(MIN_FILTER_EXPR_NUM, MAX_FILTER_EXPR_NUM + 1):
            filter_exprs = [Filter() for _ in range(num_filter_exprs)]
            filter_executor = FilterExecutor(filter_exprs=filter_exprs)
            original_cost = 0
            adaptive_cost = 0
            for i in range(NUM_BLOCKS):
                cost = filter_executor.filter(BLOCK_SIZE)
                if i == 0:
                    original_cost = cost
                elif i == NUM_BLOCKS - 1:
                    adaptive_cost = cost
            min_cost_permutation = MinCostPermutation(filter_exprs=filter_exprs)
            min_cost_permutation.permutation_min_cost(0, len(filter_exprs), filter_exprs, BLOCK_SIZE)
            print("num_filter_exprs: {}, original cost: {}, adaptive cost: {}, min cost: {}".format(num_filter_exprs, original_cost, adaptive_cost, min_cost_permutation.min_cost))
            original[num_filter_exprs] += original_cost / min_cost_permutation.min_cost
            adaptive[num_filter_exprs] += cost / min_cost_permutation.min_cost
    for i in range(MIN_FILTER_EXPR_NUM, MAX_FILTER_EXPR_NUM + 1):
        original[i] = round(original[i] / run_num, 2)
        adaptive[i] = round(adaptive[i] / run_num, 2)
    print("original:", original)
    print("adaptive:", adaptive)