using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Formatter;
using MQTTnet.Protocol;

namespace MQTT_Client_LCM
{
    internal class Program
    {
        static IMqttClient clienteDestino;
        static IMqttClient clienteOrigem;
        static TelemetriaCache cache = new TelemetriaCache();

        static readonly Dictionary<string, string> mapaTopicos = new Dictionary<string, string>
        {
            { "moto/battery", "motas/telemetria" },
            { "moto/total_kilometers", "motas/telemetria" },
            { "moto/gps/latitude", "motas/telemetria" },
            { "moto/gps/longitude", "motas/telemetria" }
        };

        static async Task Main(string[] args)
        {
            var origemOptions = new MqttClientOptionsBuilder()
                .WithTcpServer("172.20.0.201", 1884)
                .WithProtocolVersion(MqttProtocolVersion.V311)
                .Build();

            var destinoOptions = new MqttClientOptionsBuilder()
                .WithTcpServer("172.20.0.202", 1884)
                .WithClientId("MOTO999999999999")
                .WithProtocolVersion(MqttProtocolVersion.V311)
                .Build();

            var mqttFactory = new MqttFactory();
            clienteOrigem = mqttFactory.CreateMqttClient();
            clienteDestino = mqttFactory.CreateMqttClient();

            await clienteDestino.ConnectAsync(destinoOptions);
            clienteOrigem.ConnectedAsync += OnClienteOrigemLigadoAsync;
            clienteOrigem.ApplicationMessageReceivedAsync += OnMensagemRecebidaAsync;
            await clienteOrigem.ConnectAsync(origemOptions);

            Console.WriteLine("A escutar... Pressiona Enter para sair.");
            Console.ReadLine();

            await clienteOrigem.DisconnectAsync();
            await clienteDestino.DisconnectAsync();
        }

        private static async Task OnClienteOrigemLigadoAsync(MqttClientConnectedEventArgs e)
        {
            Console.WriteLine("[Origem] Ligado com sucesso.");

            foreach (var topico in mapaTopicos.Keys)
            {
                var filtro = new MqttTopicFilterBuilder()
                    .WithTopic(topico)
                    .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce)
                    .Build();

                Console.WriteLine($"[Subscrevendo] {topico}");

                await clienteOrigem.SubscribeAsync(filtro);
            }
        }

        private static async Task OnMensagemRecebidaAsync(MqttApplicationMessageReceivedEventArgs e)
        {
            var topicoOrigem = e.ApplicationMessage.Topic;
            var payload = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment.ToArray());

            Console.WriteLine($"📥 Recebido de {topicoOrigem}: {payload}");

            try
            {
                switch (topicoOrigem)
                {
                    case "moto/battery":
                        cache.Battery = int.Parse(payload, CultureInfo.InvariantCulture);
                        break;

                    case "moto/total_kilometers":
                        cache.Kilometers = double.Parse(payload.Replace(',', '.'), CultureInfo.InvariantCulture);
                        break;

                    case "moto/gps/latitude":
                        cache.Latitude = double.Parse(payload.Replace(',', '.'), CultureInfo.InvariantCulture);
                        break;

                    case "moto/gps/longitude":
                        cache.Longitude = double.Parse(payload.Replace(',', '.'), CultureInfo.InvariantCulture);
                        break;

                    default:
                        Console.WriteLine($"⚠️ Tópico não mapeado: {topicoOrigem}");
                        return;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Erro ao processar valor do tópico {topicoOrigem}: {ex.Message}");
                return;
            }

            if (cache.EstaCompleta)
            {
                var json = cache.ToJson();

                var mensagemDestino = new MqttApplicationMessageBuilder()
                    .WithTopic("motas/telemetria")
                    .WithPayload(json)
                    .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                    .WithRetainFlag(false)
                    .Build();

                try
                {
                    await clienteDestino.PublishAsync(mensagemDestino);
                    Console.WriteLine($"📤 JSON publicado para 'mota/telemetria': {json}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ Erro ao publicar para broker de destino: {ex.Message}");
                }

                cache = new TelemetriaCache(); // Limpar cache
            }
        }
    }

    class TelemetriaCache
    {
        public int? Battery;
        public double? Kilometers;
        public double? Latitude;
        public double? Longitude;

        public bool EstaCompleta =>
            Battery.HasValue && Kilometers.HasValue && Latitude.HasValue && Longitude.HasValue;

        public string ToJson()
        {
            return JsonSerializer.Serialize(new
            {
                Battery = Battery ?? 0,
                Kilometers = (int)(Kilometers ?? 0),
                Latitude = Latitude ?? 0,
                Longitude = Longitude ?? 0
            });
        }
    }
}
