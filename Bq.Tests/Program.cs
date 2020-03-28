using System;
using TrivialTestRunner;

namespace Bq.Tests
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            TRunner.AddTests<BqTests>();
            TRunner.RunTests();
        }
    }
}
