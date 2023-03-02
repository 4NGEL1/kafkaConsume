using System;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace OssProducerWithKafkaApi
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string topicName = "TransactionRssnOrAssd";

            Console.WriteLine("Demo for using Kafka APIs seamlessly with OSS");

            var config = new ProducerConfig
            {
                BootstrapServers = "r226nvs35y6q.streaming.us-ashburn-1.oci.oraclecloud.com:9092", //usually of the form cell-1.streaming.[region code].oci.oraclecloud.com:9092
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "toksopc/loyalty_streaming_user/ocid1.streampool.oc1.iad.amaaaaaafcruwjya4jdiykjqjog53t2gmouajdwvinlf2xf7r226nvs35y6q",
                SaslPassword = ">[JMQ6vg;iLzC1TeLM+-", // use the auth-token you created step 5 of Prerequisites section
                AllowAutoCreateTopics = true,
            };

            await Consume(topicName, config);
        }

        static async Task Consume(string topic, ClientConfig config)
        {
            var consumerConfig = new ConsumerConfig(config);
            consumerConfig.GroupId = "loyalty-microservice";
            consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            consumerConfig.EnableAutoCommit = true;

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
            {
                consumer.Subscribe(topic);
                var susbcribe = consumer.Subscription;
                Console.WriteLine($"This is a Name {consumer.Name}");
                Console.WriteLine($"This is a suscriptor {susbcribe.Count}");

                try
                {
                    var stop = new CancellationToken();
                    while (true)
                    {
                        Console.WriteLine("This is a iteration on bucle");
                        var cr = consumer.Consume(cts.Token);
                        string key = cr.Message.Key == null ? "Null" : cr.Message.Key;
                        Console.WriteLine($"Consumed record with key {key} and value {cr.Message.Value}");
                        await Task.Delay(5000, stop);
                        Console.WriteLine("5 Seconds of Delay");
                    }
                }
                catch (OperationCanceledException)
                {
                    //exception might have occurred since Ctrl-C was pressed.
                }
                finally
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    consumer.Close();
                }
            }
        }
    }
}