using Mh.MongoRepository.TestInstructure;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Mh.MongoRepository.Test
{
    [TestClass]
    public class AutoIncTest
    {
        static OrderIncIdRepository _repository;
        static AutoIncTest()
        {
            _repository = new OrderIncIdRepository();
        }
        [TestMethod]
        public async Task InsertTest()
        {
            var order = new OrderIncId { Name="123"};
            await _repository.InsertAsync(order);
            var result = await _repository.GetAsync(a => a.Name == "123");
            Assert.IsTrue(result.ID > 0);
        }
        [TestMethod]
        public async Task InsertBatchTest()
        {
            var list = new List<OrderIncId>();
            for (var i = 0; i < 10; i++)
            {
                var order = new OrderIncId { Name = "123" };
                list.Add(order);
            }
            await _repository.InsertBatchAsync(list);
            var result = await _repository.GetListAsync(a => a.Name == "123");
            var filter = new FilterDefinitionBuilder<OrderIncId>().Eq(nameof(OrderIncId.Name), "123");
            var result2 = await _repository.GetListAsync(filter);
            Assert.IsTrue(result.Count > 0);
            Assert.IsTrue(result2.Count > 0);
            await _repository.DeleteManyAsync(a => a.Name == "123");
        }
    }
}
