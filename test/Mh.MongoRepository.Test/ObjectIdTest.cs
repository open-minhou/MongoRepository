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
    public class ObjectIdTest
    {
        static OrderObjectIdRepository _repository;
        static ObjectIdTest()
        {
            _repository = new OrderObjectIdRepository();
        }
        [TestMethod]
        public async Task ObjectInsertTest()
        {
            var order = new OrderObjectId { Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(order);
            var result = await _repository.GetAsync(a => a.Name == order.Name);
            Assert.IsNotNull(result);
            Assert.AreEqual(result.Name, order.Name);
            Assert.IsFalse(string.IsNullOrEmpty(result.ID));
        }
        [TestMethod]
        public async Task DeleteManyTest()
        {
            var order = new OrderObjectId { Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(order);
            var filterBld = new FilterDefinitionBuilder<OrderObjectId>();
            var filter = filterBld.Eq(nameof(OrderObjectId.Name), order.Name);
            var result = await _repository.DeleteManyAsync(filter);
            Assert.IsNotNull(result);
            Assert.IsTrue(result.DeletedCount > 0);
        }
        [TestMethod]
        public async Task DeleteManyLambadaTest()
        {
            var order = new OrderObjectId { Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(order);
            var result = await _repository.DeleteManyAsync(a=>a.Name==order.Name);
            Assert.IsNotNull(result);
            Assert.IsTrue(result.DeletedCount > 0);
        }
        [TestMethod]
        public async Task DeleteOneTest()
        {
            var order = new OrderObjectId { Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(order);
            var result = await _repository.DeleteOneAsync(a => a.Name == order.Name);
            Assert.IsNotNull(result);
            Assert.IsTrue(result.DeletedCount > 0);
        }
        [TestMethod]
        public async Task DeleteOneLambadaTest()
        {
            var order = new OrderObjectId { Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(order);
            var filterBld = new FilterDefinitionBuilder<OrderObjectId>();
            var filter = filterBld.Eq(nameof(OrderObjectId.Name), order.Name);
            var result = await _repository.DeleteOneAsync(filter);
            Assert.IsNotNull(result);
            Assert.IsTrue(result.DeletedCount > 0);
        }
        [TestMethod]
        public async Task FindOneAndDeleteTest()
        {
            var order = new OrderObjectId { Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(order);
            var getResult = await _repository.GetAsync(a => a.Name == order.Name);
            Assert.IsNotNull(getResult);
            var filterBld = new FilterDefinitionBuilder<OrderObjectId>();
            var filter = filterBld.Eq(nameof(OrderObjectId.ID), getResult.ID);
            var result = await _repository.FindOneAndDeleteAsync(filter);
            Assert.IsNotNull(result);
            Assert.AreEqual(result.ID, getResult.ID);
            var deleteResult = await _repository.GetAsync(filter);
            Assert.IsNull(deleteResult);
        }
        [TestMethod]
        public async Task FindOneAndDeleteLambadaTest()
        {
            var order = new OrderObjectId { Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(order);
            var getResult = await _repository.GetAsync(a => a.Name == order.Name);
            Assert.IsNotNull(getResult);
            var result = await _repository.FindOneAndDeleteAsync(a=>a.ID==getResult.ID);
            Assert.IsNotNull(result);
            Assert.AreEqual(result.ID, getResult.ID);
            var deleteResult = await _repository.GetAsync(a => a.ID == getResult.ID);
            Assert.IsNull(deleteResult);
        }
        [TestMethod]
        public async Task FindOneAndUpdateTest()
        {
            var order = new OrderObjectId { Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(order);
            var getResult = await _repository.GetAsync(a => a.Name == order.Name);
            Assert.IsNotNull(getResult);
            var filter = new FilterDefinitionBuilder<OrderObjectId>().Eq(nameof(OrderObjectId.ID), getResult.ID);
            var newName = Guid.NewGuid().ToString("N");
            var updater = new UpdateDefinitionBuilder<OrderObjectId>().Set(nameof(OrderObjectId.Name),newName);
            var result = await _repository.FindOneAndUpdateAsync(filter,updater);
            Assert.IsNotNull(result);
            Assert.AreEqual(result.Name, order.Name);
            var newResult = await _repository.GetAsync(filter);
            Assert.AreEqual(newResult.Name, newName);
            await _repository.DeleteOneAsync(getResult.ID);
        }
        [TestMethod]
        public async Task FindOneAndUpdateLambadaTest()
        {
            var order = new OrderObjectId { Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(order);
            var getResult = await _repository.GetAsync(a => a.Name == order.Name);
            Assert.IsNotNull(getResult);
            var id = Guid.NewGuid().ToString("N");
            var updater = new UpdateDefinitionBuilder<OrderObjectId>().Set(nameof(OrderObjectId.Name), id);
            var result = await _repository.FindOneAndUpdateAsync(a=>a.ID==getResult.ID, updater);
            Assert.IsNotNull(result);
            Assert.AreEqual(result.Name, order.Name);
            var newResult = await _repository.GetAsync(a => a.ID == getResult.ID);
            Assert.AreEqual(newResult.Name,id);
            await _repository.DeleteOneAsync(getResult.ID);
        }
        [TestMethod]
        public async Task FindOneAndUpdateLambada1Test()
        {
            var order = new OrderObjectId { Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(order);
            var getResult = await _repository.GetAsync(a => a.Name ==order.Name);
            Assert.IsNotNull(getResult);
            var id = Guid.NewGuid().ToString("N");
            var updater = new OrderObjectId { Name = id};
            var result = await _repository.FindOneAndUpdateAsync(a => a.ID == getResult.ID,updater);
            Assert.IsNotNull(result);
            Assert.AreEqual(result.Name, order.Name);
            var newResult = await _repository.GetAsync(a => a.ID == getResult.ID);
            Assert.AreEqual(newResult.Name, id);
            await _repository.DeleteOneAsync(getResult.ID);
        }
        [TestMethod]
        public async Task FindOneAndUpdateLambada2Test()
        {
            var order = new OrderObjectId { Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(order);
            var getResult = await _repository.GetAsync(a => a.Name == order.Name);
            Assert.IsNotNull(getResult);
            var filter = new FilterDefinitionBuilder<OrderObjectId>().Eq(nameof(OrderObjectId.ID), getResult.ID);
            var id = Guid.NewGuid().ToString("N");
            var updater = new OrderObjectId { Name = id };
            var result = await _repository.FindOneAndUpdateAsync(filter, updater);
            Assert.IsNotNull(result);
            Assert.AreEqual(result.Name, order.Name);
            var newResult = await _repository.GetAsync(a => a.ID == getResult.ID);
            Assert.AreEqual(newResult.Name,id);
            await _repository.DeleteOneAsync(getResult.ID);
        }
        [TestMethod]
        public async Task FindOneAndUpdateFuncTest()
        {
            var order = new OrderObjectId { Name = Guid.NewGuid().ToString("N") };
            await _repository.InsertAsync(order);
            var getResult = await _repository.GetAsync(a => a.Name == order.Name);
            Assert.IsNotNull(getResult);
            var filter = new FilterDefinitionBuilder<OrderObjectId>().Eq(nameof(OrderObjectId.ID), getResult.ID);
            var id = Guid.NewGuid().ToString("N");
            var result = await _repository.FindOneAndUpdateAsync(a=>a.ID==getResult.ID, (bld) => {
                return bld.Set(nameof(OrderObjectId.Name),id);
            });
            Assert.IsNotNull(result);
            Assert.AreEqual(result.Name, order.Name);
            var newResult = await _repository.GetAsync(a => a.ID == getResult.ID);
            Assert.AreEqual(newResult.Name, id);
            await _repository.DeleteOneAsync(getResult.ID);
        }

        [TestMethod]
        public async Task InsertBatchTest()
        {
            var list = new List<OrderObjectId>();
            for (var i = 0; i < 10; i++)
            {
                var order = new OrderObjectId { Name="123" };
                list.Add(order);
            }
            await _repository.InsertBatchAsync(list);
            var result = await _repository.GetListAsync(a => a.Name == "123");
            var filter = new FilterDefinitionBuilder<OrderObjectId>().Eq(nameof(OrderObjectId.Name), "123");
            var result2 = await _repository.GetListAsync(filter);
            Assert.IsTrue(result.Count > 0);
            Assert.IsTrue(result2.Count > 0);
            await _repository.DeleteManyAsync(a => a.Name == "123");
        }
        [TestMethod]
        public async Task UpdateManyTest()
        {
            var list = new List<OrderObjectId>();
            for (var i = 0; i < 10; i++)
            {
                var order = new OrderObjectId { Name = "123" };
                list.Add(order);
            }
            await _repository.InsertBatchAsync(list);
            var updater = new UpdateDefinitionBuilder<OrderObjectId>().Set(nameof(OrderObjectId.Name), "456");
            var result = await _repository.UpdateManyAsync(a => a.Name == "123", updater);
            Assert.IsTrue(result.MatchedCount == 10);
            Assert.IsTrue(result.ModifiedCount == 10);
            var filter = new FilterDefinitionBuilder<OrderObjectId>().Eq(nameof(OrderObjectId.Name), "456");
            var updater2 = new UpdateDefinitionBuilder<OrderObjectId>().Set(nameof(OrderObjectId.Name), "789");
            var result2 = await _repository.UpdateManyAsync(filter, updater2);
            Assert.IsTrue(result2.MatchedCount ==10);
            Assert.IsTrue(result2.ModifiedCount == 10);
            var result3 = await _repository.UpdateManyAsync(a=>a.Name=="789", (bld) => {
                return bld.Set(nameof(OrderIncId.Name), "012");
            });
            Assert.IsTrue(result3.MatchedCount == 10);
            Assert.IsTrue(result3.ModifiedCount == 10);
            await _repository.DeleteManyAsync(a => a.Name == "012");
        }
    }
}
