﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using Elasticsearch.Net;
using FluentAssertions;
using Hangfire.Common;
using Hangfire.Elasticsearch.Extensions;
using Hangfire.Elasticsearch.Model;
using Hangfire.Elasticsearch.Tests.Helpers;
using Hangfire.Elasticsearch.Tests.TestInfrastructure;
using Hangfire.Storage;
using Nest;
using NUnit.Framework;

namespace Hangfire.Elasticsearch.Tests
{
    [TestFixture]
    public class ElasticConnectionIntegrationTestElasticOnline
    {
        private IElasticClient _elasticClient;
        private ElasticConnection _elasticConnection;

        [SetUp]
        public void SetUp()
        {
            _elasticClient = ElasticClientFactory.CreateClientForTest();
            _elasticConnection = new ElasticConnection(_elasticClient, new HangfireElasticSettings());
        }

        [Test]
        public void AnnounceServer_WhenServerNotExists_SavesServer()
        {
            // GIVEN
            const string serverId = "server-001";
            var serverContext = new Server.ServerContext
            {
                Queues = new[] { "queue 1", "queue 2" },
                WorkerCount = 32
            };

            // WHEN
            _elasticConnection.AnnounceServer(serverId, serverContext);

            // THEN
            var serverResponse = _elasticClient.Get<Model.Server>(serverId).ThrowIfInvalid();
            serverResponse.Found.Should().BeTrue();

            var server = serverResponse.Source;
            server.Queues.ShouldBeEquivalentTo(serverContext.Queues);
            server.WorkerCount.Should().Be(serverContext.WorkerCount);
        }

        [Test]
        public void AnnounceServer_WhenServerExists_UpdatesServer()
        {
            // GIVEN
            const string serverId = "server-001";
            _elasticClient.Index(new Model.Server { Id = serverId }, descr => descr.Refresh(Refresh.True)).ThrowIfInvalid();

            // WHEN
            var serverContext = new Server.ServerContext
            {
                Queues = new[] { "queue 1", "queue 2" },
                WorkerCount = 32
            };
            _elasticConnection.AnnounceServer(serverId, serverContext);

            // THEN
            var serverResponse = _elasticClient.Get<Model.Server>(serverId).ThrowIfInvalid();
            serverResponse.Found.Should().BeTrue();

            var server = serverResponse.Source;
            server.Queues.ShouldBeEquivalentTo(serverContext.Queues);
            server.WorkerCount.Should().Be(serverContext.WorkerCount);
        }

        [Test]
        public void RemoveServer_WithExistingServer_RemovesServer()
        {
            // GIVEN
            const string serverId = "server-001";
            _elasticClient.Index(new Model.Server { Id = serverId }, descr => descr.Refresh(Refresh.True)).ThrowIfInvalid();

            // WHEN
            _elasticConnection.RemoveServer(serverId);

            // THEN
            var getServerResponse = _elasticClient.Get<Model.Server>(serverId).ThrowIfInvalid();
            getServerResponse.Found.Should().BeFalse();
        }

        [Test]
        public void RemoveServer_WithoutExistingServer_DoesNothing()
        {
            // GIVEN
            const string serverId = "server-001";

            // WHEN
            _elasticConnection.RemoveServer(serverId);

            // THEN
            var getServerResponse = _elasticClient.Get<Model.Server>(serverId).ThrowIfInvalid();
            getServerResponse.Found.Should().BeFalse();
        }

        [Test]
        public void Heartbeat_WithExistingServer_UpdatesHeartbeat()
        {
            // GIVEN
            const string serverId = "server-001";
            var server = new Model.Server { Id = serverId, LastHeartBeat = new DateTime(2017, 10, 1) };
            _elasticClient.Index(server, descr => descr.Refresh(Refresh.True)).ThrowIfInvalid();

            // WHEN
            _elasticConnection.Heartbeat(serverId);

            // THEN
            var getServerResponse = _elasticClient.Get<Model.Server>(serverId).ThrowIfInvalid();
            getServerResponse.Found.Should().BeTrue();
            getServerResponse.Source.LastHeartBeat.Should().NotBe(server.LastHeartBeat);
        }

        [Test]
        public void Heartbeat_WithNoExistingServer_DoesNothing()
        {
            // GIVEN
            const string serverId = "server-001";

            // WHEN
            _elasticConnection.Heartbeat(serverId);

            // THEN
            var getServerResponse = _elasticClient.Count<Model.Server>(descr => descr.Query(q => q.MatchAll())).ThrowIfInvalid();
            getServerResponse.Count.Should().Be(0);
        }

        [Test]
        public void RemoveTimedOutServers_RemovesTimedOutServer()
        {
            // GIVEN
            const string serverId = "server-001";
            var server = new Model.Server { Id = serverId, LastHeartBeat = new DateTime(2000, 1, 1) };
            _elasticClient.Index(server, descr => descr.Refresh(Refresh.True)).ThrowIfInvalid();

            // WHEN
            var removedServersCount = _elasticConnection.RemoveTimedOutServers(TimeSpan.FromSeconds(1));

            // THEN
            removedServersCount.Should().Be(1);
            var getServerResponse = _elasticClient.Get<Model.Server>(serverId).ThrowIfInvalid();
            getServerResponse.Found.Should().BeFalse();
        }

        [Test]
        public void RemoveTimedOutServers_WithLotsOfTimedOutServers_RemovesTimedOutServer()
        {
            // GIVEN
            const int elasticMaxResponseDocumentCount = 5000;
            var servers = Enumerable.Range(0, elasticMaxResponseDocumentCount + 1)
                .Select(i => new Model.Server { Id = $"server-{i}", LastHeartBeat = new DateTime(2000, 1, 1) });
            _elasticClient.IndexMany(servers).ThrowIfInvalid();
            _elasticClient.Refresh(Indices.All).ThrowIfInvalid();

            // WHEN
            var removedServersCount = _elasticConnection.RemoveTimedOutServers(TimeSpan.FromSeconds(1));

            // THEN
            removedServersCount.Should().Be(elasticMaxResponseDocumentCount + 1);
            _elasticClient.Refresh(Indices.All).ThrowIfInvalid();
            var getServerResponse = _elasticClient.Count<Model.Server>(descr => descr.Query(q => q.MatchAll())).ThrowIfInvalid();
            getServerResponse.Count.Should().Be(0);
        }

        [Test]
        public void RemoveTimedOutServers_WithNoTimedOutServers_DoesNothing()
        {
            // GIVEN
            const string serverId = "server-001";
            var server = new Model.Server { Id = serverId, LastHeartBeat = DateTime.UtcNow };
            _elasticClient.Index(server, descr => descr.Refresh(Refresh.True)).ThrowIfInvalid();

            // WHEN
            var removedServersCount = _elasticConnection.RemoveTimedOutServers(TimeSpan.FromDays(1));

            // THEN
            removedServersCount.Should().Be(0);
            var getServerResponse = _elasticClient.Get<Model.Server>(serverId).ThrowIfInvalid();
            getServerResponse.Found.Should().BeTrue();
            getServerResponse.Source.ShouldBeEquivalentTo(server);
        }

        [Test]
        public void RemoveTimedOutServers_WithNegativeTimeout_Throws()
        {
            // GIVEN
            var timeout = TimeSpan.FromSeconds(-1);

            // WHEN THEN
            Assert.Throws<ArgumentException>(() => _elasticConnection.RemoveTimedOutServers(timeout));
        }

        [Test]
        public void GetAllItemsFromSet_WithNullKey_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => _elasticConnection.GetAllItemsFromSet(null));
        }

        [Test]
        public void GetAllItemsFromSet_ReturnsExpectedItems()
        {
            // GIVEN
            var sets = new[]
            {
                new Set
                {
                    Id = "key-1",
                    SetValues = new[]
                    {
                        new SetValue {Value = "value-1"},
                        new SetValue {Value = "value-2"},
                    }
                },
                new Set
                {
                    Id = "key-2",
                    SetValues = new[]
                    {
                        new SetValue {Value = "value-3"}
                    }
                }
            };
            _elasticClient.IndexMany(sets).ThrowIfInvalid();
            _elasticClient.Refresh(Indices.All).ThrowIfInvalid();

            // WHEN
            var set = _elasticConnection.GetAllItemsFromSet("key-1");

            // THEN
            set.Count.Should().Be(2);
            set.ShouldAllBeEquivalentTo(new[] { "value-1", "value-2" });
        }

        [Test]
        public void GetFirstByLowestScoreFromSet_WithNullKey_Throws()
        {
            // GIVEN
            const string key = null;
            const double fromScore = 10;
            const double toScore = 15;

            // WHEN THEN
            Assert.Throws<ArgumentNullException>(() => _elasticConnection.GetFirstByLowestScoreFromSet(key, fromScore, toScore));
        }

        [Test]
        public void GetFirstByLowestScoreFromSet_WithFromScore_HigherThan_ToScore_Throws()
        {
            // GIVEN
            const string key = "key";
            const double fromScore = 15;
            const double toScore = 10;

            // WHEN THEN
            Assert.Throws<ArgumentException>(() => _elasticConnection.GetFirstByLowestScoreFromSet(key, fromScore, toScore));
        }

        [Test]
        public void GetFirstByLowestScoreFromSet_ReturnsExpectedItem()
        {
            // GIVEN
            const string key = "key-1";
            var set = new Set
            {
                Id = key,
                SetValues = new[]
                {
                    new SetValue {Value = "value-1", Score = 10},
                    new SetValue {Value = "value-2", Score = 15},
                    new SetValue {Value = "value-3", Score = 20},
                }
            };
            _elasticClient.Index(set, desc => desc.Refresh(Refresh.True)).ThrowIfInvalid();

            // WHEN
            var value = _elasticConnection.GetFirstByLowestScoreFromSet(key, 13, 17);

            // THEN
            value.Should().NotBeNull();
            value.Should().Be("value-2");
        }

        [Test]
        public void GetFirstByLowestScoreFromSet_WithNoMatchingSet_ReturnsNull()
        {
            // GIVEN
            const string key = "key-1";

            // WHEN
            var value = _elasticConnection.GetFirstByLowestScoreFromSet(key, 10, 15);

            // THEN
            value.Should().BeNull();
        }

        [Test]
        public void GetFirstByLowestScoreFromSet_WithNoMatchingScores_ReturnsNull()
        {
            // GIVEN
            const string key = "key-1";
            var set = new Set
            {
                Id = key,
                SetValues = new[]
                {
                    new SetValue {Value = "value-1", Score = 10},
                }
            };
            _elasticClient.Index(set, desc => desc.Refresh(Refresh.True)).ThrowIfInvalid();

            // WHEN
            var value = _elasticConnection.GetFirstByLowestScoreFromSet(key, 15, 20);

            // THEN
            value.Should().BeNull();
        }

        [Test]
        public void GetAllEntriesFromHash_ReturnsExpectedResults()
        {
            // GIVEN
            const string key = "key-1";
            var hash = new Hash
            {
                Id = key,
                Hashes = new Dictionary<string, string>
                {
                    {"hash-1", "value-1"},
                    {"hash-2", "value-2"},
                    {"hash-3", "value-3"},
                }
            };
            _elasticClient.Index(hash, desc => desc.Refresh(Refresh.True)).ThrowIfInvalid();

            // WHEN
            var actualHashes = _elasticConnection.GetAllEntriesFromHash(key);

            // THEN
            actualHashes.ShouldBeEquivalentTo(hash.Hashes);
        }

        [Test]
        public void GetAllEntriesFromHash_GivenNonExistingKey_ReturnsNull()
        {
            // GIVEN
            const string key = "key-1";

            // WHEN
            var hash = _elasticConnection.GetAllEntriesFromHash(key);

            // THEN
            hash.Should().BeNull();
        }

        [Test]
        public void GetAllEntriesFromHash_GivenNullKey_Throws()
        {
            // GIVEN WHEN THEN
            Assert.Throws<ArgumentNullException>(() => _elasticConnection.GetAllEntriesFromHash(null));
        }

        [Test]
        public void SetRangeInHash_SetsExpectedValues()
        {
            // GIVEN
            const string key = "key-1";
            var hash = new Hash
            {
                Id = key,
                Hashes = new Dictionary<string, string>
                {
                    {"hash-1", "value-1"},
                    {"hash-2", "value-2"}
                }
            };
            _elasticClient.Index(hash, desc => desc.Refresh(Refresh.True)).ThrowIfInvalid();

            // WHEN
            var newValues = new Dictionary<string, string>
            {
                {"hash-2", "value-2b" },
                {"hash-3", "value-3" }
            };
            _elasticConnection.SetRangeInHash(key, newValues);

            // THEN
            var actualHashes = _elasticClient.Get<Hash>(key).ThrowIfInvalid();
            var expectedHashes = new Dictionary<string, string>
            {
                {"hash-1", "value-1"},
                {"hash-2", "value-2b" },
                {"hash-3", "value-3" }
            };
            actualHashes.Source.Hashes.ShouldBeEquivalentTo(expectedHashes);
        }

        [Test]
        public void SetRangeInHash_GivenNonExistingKey_CreatesNewHash()
        {
            // GIVEN
            const string key = "key-1";
            var newValues = new Dictionary<string, string>
            {
                {"hash-1", "value-1" },
                {"hash-2", "value-2" }
            };

            // WHEN
            _elasticConnection.SetRangeInHash(key, newValues);

            // THEN
            var actualHashes = _elasticClient.Get<Hash>(key).ThrowIfInvalid();
            actualHashes.Source.Hashes.ShouldBeEquivalentTo(newValues);
        }

        [Test]
        public void SetRangeInHash_GivenNullKey_Throws()
        {
            // GIVEN WHEN THEN
            Assert.Throws<ArgumentNullException>(() => _elasticConnection.SetRangeInHash(null, new List<KeyValuePair<string, string>>()));
        }

        [Test]
        public void SetRangeInHash_GivenNullKeyValuePairs_Throws()
        {
            // GIVEN WHEN THEN
            const string key = "key-1";
            Assert.Throws<ArgumentNullException>(() => _elasticConnection.SetRangeInHash(key, null));
        }

        [Test]
        public void GetStateData_GivenNullKey_Throws()
        {
            // GIVEN WHEN THEN
            Assert.Throws<ArgumentNullException>(() => _elasticConnection.GetStateData(null));
        }

        [Test]
        public void GetStateData_GivenExistingJobStateData_ReturnsExpectedResults()
        {
            // GIVEN 
            const string key = "key-1";
            var jobStateData = new StateDataDto
            {
                Name = "Name-1",
                Reason = "Reason-1",
                Data = new Dictionary<string, string>
                {
                    {"data-1", "value-1"},
                    {"data-2", "value-2"},
                    {"data-3", "value-3"}
                }
            };
            var jobData = new JobDataDto { Id = key, StateDataDto = jobStateData };
            _elasticClient.Index(jobData, desc => desc.Refresh(Refresh.True)).ThrowIfInvalid();

            // WHEN
            var actualStateData = _elasticConnection.GetStateData(key);

            // THEN
            actualStateData.Should().NotBeNull();
            actualStateData.ShouldBeEquivalentTo(jobStateData.ToStateData());
        }

        [Test]
        public void GetStateData_GivenNullJobStateDto_ReturnsExpectedResults()
        {
            // GIVEN 
            const string key = "key-1";
            var jobData = new JobDataDto { Id = key, StateDataDto = null };
            _elasticClient.Index(jobData, desc => desc.Refresh(Refresh.True)).ThrowIfInvalid();

            // WHEN
            var actualStateData = _elasticConnection.GetStateData(key);

            // THEN
            actualStateData.Should().BeNull();
        }

        [Test]
        public void GetStateData_GivenNonExistingJobStateDto_ReturnsNull()
        {
            // GIVEN 
            const string key = "key-1";

            // WHEN
            var actualStateData = _elasticConnection.GetStateData(key);

            // THEN
            actualStateData.Should().BeNull();
        }

        [Test]
        public void GetJobData_GivenNullKey_Throws()
        {
            // GIVEN WHEN THEN
            Assert.Throws<ArgumentNullException>(() => _elasticConnection.GetJobData(null));
        }

        [Test]
        public void GetJobData_GivenNonExistingJobData_ReturnsNull()
        {
            // GIVEN 
            const string key = "key-1";

            // WHEN
            var actualStateData = _elasticConnection.GetJobData(key);

            // THEN
            actualStateData.Should().BeNull();
        }

        [Test]
        public void GetJobData_GivenExistingJobData_ReturnsExpectedObject()
        {
            // GIVEN 
            const string key = "key-1";
            var jobData = new JobDataDto
            {
                Id = key,
                StateName = "StateName-1",
                CreatedAt = new DateTime(2017, 11, 1),
                InvocationDataDto = new InvocationDataDto()
            };

            _elasticClient.Index(jobData, desc => desc.Refresh(Refresh.True)).ThrowIfInvalid();

            // WHEN
            var actualJobData = _elasticConnection.GetJobData(key);

            // THEN
            actualJobData.Should().NotBeNull();
            actualJobData.CreatedAt.Should().Be(jobData.CreatedAt);
            actualJobData.State.Should().Be(jobData.StateName);
            actualJobData.LoadException.Should().NotBeNull();
        }

        [Test]
        public void GetJobParameter_WithNullId_Throws()
        {
            // GIVEN WHEN THEN
            Assert.Throws<ArgumentNullException>(() => _elasticConnection.GetJobParameter(null, "name"));
        }

        [Test]
        public void GetJobParameter_WithNullName_Throws()
        {
            // GIVEN WHEN THEN
            Assert.Throws<ArgumentNullException>(() => _elasticConnection.GetJobParameter("id", null));
        }

        [Test]
        public void GetJobParameter_GivenNonExistingJobData_ReturnsNull()
        {
            // GIVEN 
            const string key = "key-1";

            // WHEN
            var actualJobParameter = _elasticConnection.GetJobParameter(key, "name");

            // THEN
            actualJobParameter.Should().BeNull();
        }

        [Test]
        public void GetJobParameter_GivenExistingJobDataAndParameterName_ReturnsValue()
        {
            // GIVEN 
            const string key = "key-1";
            const string parameterName = "parameter-1";
            const string parameterValue = "value-1";
            var jobData = new JobDataDto
            {
                Id = key,
                JobParameters = new Dictionary<string, string>
                {
                    {parameterName, parameterValue}
                }
            };
            _elasticClient.Index(jobData, desc => desc.Refresh(Refresh.True)).ThrowIfInvalid();

            // WHEN
            var actualJobParameter = _elasticConnection.GetJobParameter(key, parameterName);

            // THEN
            actualJobParameter.Should().Be(parameterValue);
        }

        [Test]
        public void GetJobParameter_GivenExistingJobDataAndNonExistingParameterName_ReturnsNull()
        {
            // GIVEN 
            const string key = "key-1";
            var jobData = new JobDataDto
            {
                Id = key,
                JobParameters = new Dictionary<string, string>()
            };
            _elasticClient.Index(jobData, desc => desc.Refresh(Refresh.True)).ThrowIfInvalid();

            // WHEN
            var actualJobParameter = _elasticConnection.GetJobParameter(key, "parameter-1");

            // THEN
            actualJobParameter.Should().BeNull();
        }

        [Test]
        public void SetJobParameter_WithNullId_Throws()
        {
            // GIVEN WHEN THEN
            Assert.Throws<ArgumentNullException>(() => _elasticConnection.SetJobParameter(null, "name", "value"));
        }

        [Test]
        public void SetJobParameter_WithNullName_Throws()
        {
            // GIVEN WHEN THEN
            Assert.Throws<ArgumentNullException>(() => _elasticConnection.SetJobParameter("id", null, "value"));
        }

        [Test]
        public void SetJobParameter_GivenExistingJobData_SetsValue()
        {
            // GIVEN 
            const string key = "key-1";
            var jobData = new JobDataDto
            {
                Id = key,
                JobParameters = new Dictionary<string, string>()
            };
            _elasticClient.Index(jobData, desc => desc.Refresh(Refresh.True)).ThrowIfInvalid();

            // WHEN
            const string parameterName = "parameter-1";
            const string parameterValue = "value";
            _elasticConnection.SetJobParameter(key, parameterName, parameterValue);

            // THEN
            var jobDataResponse = _elasticClient.Get<JobDataDto>(key).ThrowIfInvalid();
            var jobDataDto = jobDataResponse.Source;
            jobDataDto.JobParameters.Should().HaveCount(1);
            jobDataDto.JobParameters.Should().ContainKey(parameterName);
            jobDataDto.JobParameters[parameterName].Should().Be(parameterValue);
        }

        [Test]
        public void SetJobParameter_GivenExistingJobDataWithExistingParameterName_UpdatesValue()
        {
            // GIVEN 
            const string key = "key-1";
            const string parameterName = "parameter-1";
            var jobData = new JobDataDto
            {
                Id = key,
                JobParameters = new Dictionary<string, string>
                {
                    {parameterName, "value" }
                }
            };
            _elasticClient.Index(jobData, desc => desc.Refresh(Refresh.True)).ThrowIfInvalid();

            // WHEN
            const string newParameterValue = "new value";
            _elasticConnection.SetJobParameter(key, parameterName, newParameterValue);

            // THEN
            var jobDataResponse = _elasticClient.Get<JobDataDto>(key).ThrowIfInvalid();
            var jobDataDto = jobDataResponse.Source;
            jobDataDto.JobParameters.Should().HaveCount(1);
            jobDataDto.JobParameters.Should().ContainKey(parameterName);
            jobDataDto.JobParameters[parameterName].Should().Be(newParameterValue);
        }

        [Test]
        public void SetJobParameter_GivenNonExistingJobData_DoesNothing()
        {
            // GIVEN 
            const string key = "key-1";

            // WHEN
            _elasticConnection.SetJobParameter(key, "parameter-1", "value");

            // THEN
            var jobDataResponse = _elasticClient.Get<JobDataDto>(key).ThrowIfInvalid();
            jobDataResponse.Found.Should().BeFalse();
        }

        [Test]
        public void FetchNextJob_GivenNullQueues_Throws()
        {
            // GIVEN WHEN THEN
            Assert.Throws<ArgumentNullException>(() => _elasticConnection.FetchNextJob(null, CancellationToken.None));
        }

        [Test]
        public void FetchNextJob_GivenEmptyQueues_Throws()
        {
            // GIVEN WHEN THEN
            Assert.Throws<ArgumentException>(() => _elasticConnection.FetchNextJob(new string[0], CancellationToken.None));
        }

        [Test]
        public void FetchNextJob_FetchesNextJobInQueue()
        {
            // GIVEN
            const string queue = "default";
            var jobData = new JobDataDto
            {
                Id = "job-1",
                Queue = queue,
                CreatedAt = new DateTime(2017, 11, 22)
            };
            _elasticClient.Index(jobData, desc => desc.Refresh(Refresh.True)).ThrowIfInvalid();

            // WHEN
            var job = _elasticConnection.FetchNextJob(new[] { queue }, CancellationToken.None);

            // THEN
            job.Should().NotBeNull();

            var fetchedJob = job as FetchedJob;
            fetchedJob.Should().NotBeNull();
            fetchedJob.JobId.Should().Be(jobData.Id);
        }

        [Test]
        public void FetchNextJob_FetchesNextJobWhenAvailable()
        {
            // GIVEN
            const string queue = "default";
            var jobData = new JobDataDto
            {
                Id = "job-1",
                Queue = queue,
                CreatedAt = new DateTime(2017, 11, 22)
            };

            // WHEN
            var task = TaskHelper.RunTimedTask(() => _elasticConnection.FetchNextJob(new[] { queue }, CancellationToken.None));

            var timeToWait = TimeSpan.FromSeconds(5);
            Thread.Sleep(timeToWait);
            _elasticClient.Index(jobData, desc => desc.Refresh(Refresh.True)).ThrowIfInvalid();

            task.Wait();

            // THEN
            var executionTime = task.Result.ExecutionDuration;
            var fetchedJob = task.Result.Result;

            fetchedJob.Should().NotBeNull();
            var hfFetchedJob = fetchedJob as FetchedJob;
            hfFetchedJob.Should().NotBeNull();
            hfFetchedJob.JobId.Should().Be(jobData.Id);

            executionTime.Should().BeGreaterOrEqualTo(timeToWait);
        }

        [Test]
        public void FetchNextJob_ReturnsNull_WhenNoJobAvailable_AndCancellationRequested()
        {
            // GIVEN
            const string queue = "default";
            var cancellationToken = new CancellationTokenSource();

            // WHEN
            var task = TaskHelper.RunTimedTask(() => _elasticConnection.FetchNextJob(new[] { queue }, cancellationToken.Token));

            var timeToWait = TimeSpan.FromSeconds(5);
            cancellationToken.CancelAfter(timeToWait);

            task.Wait();

            // THEN
            var executionTime = task.Result.ExecutionDuration;
            var fetchedJob = task.Result.Result;

            fetchedJob.Should().BeNull();
            executionTime.Should().BeGreaterOrEqualTo(timeToWait);
        }

        [Test]
        public void FetchNextJob_ReturnsNull_WhenNoJobAvailableInSpecifiedQueue_AndCancellationRequested()
        {
            // GIVEN
            const string queue = "queue-1";
            var jobData = new JobDataDto
            {
                Id = "job-1",
                Queue = "queue-2",
                CreatedAt = new DateTime(2017, 11, 22)
            };
            _elasticClient.Index(jobData, desc => desc.Refresh(Refresh.True)).ThrowIfInvalid();
            var cancellationToken = new CancellationTokenSource();

            // WHEN
            var task = TaskHelper.RunTimedTask(() => _elasticConnection.FetchNextJob(new[] { queue }, cancellationToken.Token));

            var timeToWait = TimeSpan.FromSeconds(5);
            cancellationToken.CancelAfter(timeToWait);

            task.Wait();

            // THEN
            var executionTime = task.Result.ExecutionDuration;
            var fetchedJob = task.Result.Result;

            fetchedJob.Should().BeNull();
            executionTime.Should().BeGreaterOrEqualTo(timeToWait);
        }

        [Test]
        public void CreateExpiredJob_GivenNullJob_Throws()
        {
            // GIVEN WHEN THEN
            Assert.Throws<ArgumentNullException>(() => _elasticConnection.CreateExpiredJob(null, new Dictionary<string, string>(), new DateTime(), new TimeSpan()));
        }

        [Test]
        public void CreateExpiredJob_GivenNullParameters_Throws()
        {
            // GIVEN 
            var currentMethod = GetType().GetMethod(nameof(CreateExpiredJob_GivenNullJob_Throws));

            // WHEN THEN
            Assert.Throws<ArgumentNullException>(() => _elasticConnection.CreateExpiredJob(new Job(currentMethod), null, new DateTime(), new TimeSpan()));
        }
    }
}