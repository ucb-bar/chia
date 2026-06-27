#include <stdio.h>
#include "rocc.h"
// See LICENSE for license details.

#define DATA_SIZE 100

#define read_csr(reg) ({ unsigned long __tmp; \
  asm volatile ("csrr %0, " #reg : "=r"(__tmp)); \
  __tmp; })

#define rdcycle() read_csr(cycle)

typedef long long data_t;

int main(void)
{
    static data_t input_data[DATA_SIZE] = {
        0, 15, 10, 3, 14, 6, 2, 18, 11, 15, 11, 0, 17, 16, 7, 13, 18, 2, 2, 5, 
        8, 5, 12, 14, 6, 12, 16, 7, 9, 17, 10, 10, 3, 5, 14, 11, 9, 12, 3, 1, 
        5, 4, 6, 17, 17, 4, 17, 15, 17, 18, 3, 18, 10, 6, 12, 12, 3, 2, 16, 1, 
        5, 6, 2, 17, 16, 5, 10, 18, 14, 4, 9, 9, 7, 4, 8, 13, 12, 6, 4, 17, 
        5, 2, 11, 11, 7, 6, 8, 5, 6, 6, 11, 1, 18, 7, 6, 9, 10, 17, 14, 4
    };

    static data_t output_data[DATA_SIZE];

    unsigned long result = 0;
    
    printf("Address of array (input_data): %p (output_data): %p\n", input_data, output_data); 


	ROCC_INSTRUCTION_DSS(1, result, input_data, output_data, 0);
    if (result != 1) {
        printf("MEMCPY Load Addresses Instruction Failed\n");
        return 0;
    }
    uint64_t t1 = rdcycle();
    ROCC_INSTRUCTION_DS(1, result, DATA_SIZE, 1);

    if (result != 1) {
        printf("MEMCPY Start Instruction Failed\n");
        return 0;
    }

    uint64_t t2 = rdcycle();

    int num_correct = 0;
    
    for (int i = 0; i < DATA_SIZE; i++) {
        if (output_data[i] == input_data[i]) {
            num_correct += 1;
        }
    }

    printf("MEMCPY Num Correct: %d\n", num_correct);
    printf("MEMCPY Cycles Taken: %lu\n", t2 - t1);
    return 0;
}
