using System;
using System.Threading.Tasks;
using TrivialTestRunner;

namespace Bq.Tests
{
    static class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            TRunner.AddTests<BqTests>();
            await TRunner.RunTestsAsync();
        }
    }
}
