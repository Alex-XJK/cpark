# Getting Started with cpark: A Step-by-Step Tutorial

> CPARK develop team - December 7, 2023

## 1. Check Documentation Website

Visit the [cpark documentation website](https://www.alexxu.tech/cpark/) for comprehensive information, guides, and examples.  

Especially, on the [homepage](https://www.alexxu.tech/cpark/index.html), there is a general introduction of the whole project and a brief version of how to get started with this project.
On the [Related Pages](https://www.alexxu.tech/cpark/md_use__cases.html), there is a screenshot of a real-world shell session to build your first program from scratch.
The [Topics](https://www.alexxu.tech/cpark/topics.html) page is the main user entry for detailed technical documentation of different types of Creations, Transformations, and Actions, as explained in our design documentation.

## 2. Get cpark

Clone the cpark repository to your local machine:

```bash
git clone https://github.com/Alex-XJK/cpark.git
```

Make sure you are in the main-branch, which has been passing all the automation tests enforced by our branch policy everytime a pull request is trying to merge into main-branch. 

## 3. Write Your First cpark Code

With our delicate cpark library on your computer, we can now build some great program using it.  
First, please create a C++ file, for example, `cpark.cpp`, and add the following code:

```c
#include <iostream>
// Include the cpark header files you used
#include "generator_rdd.h"
#include "transformed_rdd.h"
#include "filter_rdd.h"
#include "reduce.h"

int main() {
  // Set up configuration and execution context
  cpark::Config default_config;
  default_config.setParallelTaskNum(2); // Set to use 2 cores first
  cpark::ExecutionContext default_context{default_config};

  // Define a cpark computation pipeline
  auto cpark_result =
      cpark::GeneratorRdd(1, 5000000, [&](auto i) -> auto { return i; }, &default_context) |
      cpark::Transform([](auto x) { return x * x; }) |
      cpark::Transform([](auto x) {
        int res = 0;
        for (int i = 1; i <= x; i++) res += x;
        return res;
      }) |
      cpark::Filter([](auto x) { return x % 5 == 0; }) |
      cpark::Reduce([](auto x, auto y) { return x + y; });

  // Print the result
  std::cout << "The computation result is " << cpark_result << std::endl;

  return 0;
}
```

## 4. Configure Your Compiler

Since our project makes the use of the C++ 20 standard library <ranges> and other new features, you have to ensure that your local compiler supports C++20 and set the correct compilation flag. For example: `-std=c++20`.  

You also have to manage your compiler to look for header files in the correct location.  

So, for this example program, I can compile with `g++` on my local computer with the following command:

```shell
g++ cpark.cpp -std=c++20 -Wall -Wpedantic -O2 -I./cpark/include -o cpark_example
```

## 5. Run Your cpark Code

Execute the compiled example:

```shell
./cpark_example
```

And you can see the running result as

```text
The computation result is -322035642
```

While this result number doesn't make sense, because it gets overflow, thus the running result is an undefined behavior,
we purposely choose such a large number here for performance comparison below.

## 6. Modify Running Configuration

Noticed that, in your previous code, we were very conservative in choosing 2 cpu cores for the calculation.
So now let's run the executable again and see how well it performs: 
```text
Home >> time ./cpark_example
The computation result is -322035642
./cpark_example  0.06s user 0.00s system 173% cpu 0.036 total
```
the `time` command reports to us that it used 173% CPU, which is very close to the parameter we set for 2 cores, at which the program took a total of 0.036 seconds.  

If your computer has more CPU cores, you can also change this parameter to try other parallelism levels.  

Of course, there is another trick here. If we check the documentation for [this function](https://www.alexxu.tech/cpark/classcpark_1_1_config.html#a0463ce1e02a8cea29e685544b8813e7f), 
we can see that if we use the default value for this function, we can have cpark library automatically check the number of parallelisms supported by the current hardware setup. 
```c
// Set up configuration and execution context
cpark::Config default_config;
default_config.setParallelTaskNum(); // Default to physical supported thread number.
cpark::ExecutionContext default_context{default_config};
// Other code ...
```
So after we changed the relevant parameters, we can experiment again. 
```text
Home >> g++ cpark.cpp -std=c++20 -Wall -Wpedantic -O2 -I./cpark/include -o cpark_example
Home >> time ./cpark_example
The computation result is -322035642
./cpark_example  0.06s user 0.00s system 665% cpu 0.009 total
```
It is obvious that it runs significantly faster and the CPU usage is really close to what my computer expected.

## Conclusion: Unleash the Power of Parallel Computing with cpark

Congratulations! You've successfully embarked on your journey with cpark, a high-performance parallel computing library. As you explore the documentation, you'll discover a wealth of features and capabilities that empower you to tackle complex computations with ease. From parallelized data generation to seamless result reduction, cpark is designed to streamline your code and unlock the full potential of your machine's capabilities.

Whether you're a seasoned developer or new to parallel computing, cpark offers a user-friendly yet powerful framework to elevate your C++ projects. Dive into the documentation, experiment with different pipelines, and leverage the flexibility of cpark to optimize your computations.

Happy coding, and may your parallel computations be as efficient as they are exciting!
