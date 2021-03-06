﻿using System;
using System.Threading.Tasks;
using TrivialTestRunner;

namespace Bq.Tests.Integration
{
    class Program
    {
        static async Task Main(string[] args)
        {
            TRunner.AddTests<BqIntegrationTests>();
            TRunner.CrashHard = true;
            await TRunner.RunTestsAsync();
            TRunner.ReportAll();
            Console.ReadLine();
        }
    }
}
