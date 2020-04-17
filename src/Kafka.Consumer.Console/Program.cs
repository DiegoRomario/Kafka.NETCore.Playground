using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Consumer
{
    class Program
    {
        static Task Main(string[] args)
        {
            Console.WriteLine("Starting the service...");
            Thread.Sleep(2000);
            Console.WriteLine("Service is running...");
            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe("my_demo_topic");
                var cts = new CancellationTokenSource();

                try
                {
                    while (true)
                    {
                        ConsumeResult<Ignore, string> message = c.Consume(cts.Token);
                        Console.WriteLine($"Message: {message.Message.Value} received from {message.TopicPartitionOffset}");
                    }
                }
                catch (OperationCanceledException)
                {
                    c.Close();
                }
            }

            return Task.CompletedTask;
        }
    }
}
