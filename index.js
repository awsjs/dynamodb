const _ = require('lodash');
const AWS = require('aws-sdk');
const { assert } = require('chai');

class DynamoDB {
  constructor ({ table, env = process.env.NODE_ENV, region = process.env.REGION }) {
    this.table = `lo-${table}-${env}`;
    this.ddb = new AWS.DynamoDB.DocumentClient({ region });
  }

  async get (key) {
    const params = {
      TableName: this.table,
      Key: key
    };
    const response = await this.ddb.get(params).promise();
    return response.Item;
  }

  async create ({ data, userId }) {
    assert.isDefined(data.id);
    data = {
      ...data,
      meta: {
        version: 1,
        createdAt: new Date().toISOString(),
        createdBy: userId
      }
    };
    const params = {
      TableName: this.table,
      Item: data,
      ConditionExpression: 'attribute_not_exists(id)'
    };
    await this.ddb.put(params).promise();
    return data;
  }

  async update ({ key, data, userId }) {
    assert.isObject(key);
    assert.isObject(data);
    assert.isDefined(userId);
    let params;
    try {
      const ean = { '#meta': 'meta', '#version': 'version', '#modifiedAt': 'modifiedAt', '#modifiedBy': 'modifiedBy' };
      const set = ['#meta.#modifiedBy = :userId', '#meta.#modifiedAt = :now'];
      const attrs = { ':userId': userId, ':now': new Date().toISOString(), ':increment': 1 };
      let varindex = 1;
      for (const name in data) {
        const value = data[name];
        if (value === undefined) continue;
        attrs[`:var_${varindex}`] = value;
        name.split('.').forEach(n => { ean[`#${n}`] = n; });
        set.push(`${name.split('.').map(n => `#${n}`).join('.')} = :var_${varindex}`);
        varindex += 1;
      }
      params = {
        TableName: this.table,
        Key: key,
        UpdateExpression: `SET ${set.join(', ')} ADD #meta.#version :increment`,
        ExpressionAttributeValues: attrs,
        ExpressionAttributeNames: ean,
        ReturnValues: 'ALL_NEW',
        ConditionExpression: 'attribute_exists(id)'
      };
      const result = await this.ddb.update(params).promise();
      return result.Attributes;
    } catch (e) {
      console.log('params: ', params);
      console.error(e);
      throw e;
    }
  }

  async delete (key) {
    const params = {
      TableName: this.table,
      Key: key
    };
    await this.ddb.delete(params).promise();
  }

  async query ({ index, expression, filter, values, attrs }) {
    // TODO iterate via all items
    const params = {
      TableName: this.table,
      KeyConditionExpression: expression,
      ExpressionAttributeValues: _.reduce(values, (p, v, k) => ({ ...p, [`:${k}`]: v }), {})
    };
    if (index) params.IndexName = index;
    if (filter) params.FilterExpression = filter;
    if (attrs) params.ExpressionAttributeNames = attrs;
    const response = await this.ddb.query(params).promise();
    return response.Items;
  }

  async scan ({ projection, expression, values } = {}) {
    let list = [];
    const params = {
      TableName: this.table,
      ProjectionExpression: projection
    };
    if (expression) params.FilterExpression = expression;
    if (values) {
      params.ExpressionAttributeValues = {};
      for (const k in values) params.ExpressionAttributeValues[`:${k}`] = values[k];
    }
    do {
      const response = await this.ddb.scan(params).promise();
      params.ExclusiveStartKey = response.LastEvaluatedKey;
      list = [...list, ...response.Items];
    } while (params.ExclusiveStartKey);
    return list;
  }

}

module.exports = DynamoDB;
