using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using TwitterSearchAPI;
using TwitterSearchAPI.Models;

namespace WorkServiceTwitt
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private List<Tweet> tweets;
       

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            tweets = new List<Tweet>();
            return base.StartAsync(cancellationToken);
        }

        // no need to close database connection starting from v2.0.1.27 for MongoDB.Driver (i am using a 3.2.0 version)
        // because the client handles the connection dispose automatically
        // so it isn't necessary to override the "Task StopAsync" method



        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {

                var extractor = new TweetExtractor(new HttpClient());
                // Run extractor
                await extractor.SearchTweetsAsync(
                // Create the search query in order to search the infos in twitter
                 new SearchExecutionInfo
                 {
                      Query = "#startup"
                 },
                      // Stop the extractor after we get 30 items at least and we put tweets to a list
                      canExecute: () => tweets.Count <= 30,
                      onTweetsExtracted: results =>
                      {
                          tweets.AddRange(results);
                      });

                // mongo db connection
                 var client = new MongoClient();
                 var db = client.GetDatabase("twitter");
                 var collec = db.GetCollection<TwitterEnt>("startup");

                // add the tweet if it exists
                 foreach (var item in tweets)
                 {
                     //Console.WriteLine(item.Id);
                     var list = await collec.Find(x => x._id == item.Id.ToString()).ToListAsync();
                     if (list.Count() == 0)
                         await collec.InsertOneAsync(new TwitterEnt { _id = item.Id.ToString(), Text = item.Text });
                 }
                tweets.Clear();




                // logging information
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                await Task.Delay(10000, stoppingToken);
            }
        }

        public class TwitterEnt
        {
            public string _id { get; set; }
            public string Text { get; set; }
        }
    }
}
