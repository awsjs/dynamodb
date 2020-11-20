const _ = require('lodash');
const AWS = require('aws-sdk');
const { assert } = require('chai');

class DynamoDB {
  constructor ({ table }) {
    this.table = table;
    this.ddb = new AWS.DynamoDB.DocumentClient();
  }

  async get (key) {
    try {
      const params = {
        TableName: this.table,
        Key: key
      };
      const response = await this.ddb.get(params).promise();
      return response.Item;
    } catch (e) {
      // if (e.code === 'ResourceNotFoundException') return null;
      console.error(e);
      throw e;
    }
  }

  async put (data) {
    const params = {
      TableName: this.table,
      Item: data
    };
    await this.ddb.put(params).promise();
    return data;
  }

  async create ({ data, userId }) {
    assert.isDefined(data.id);
    const params = {
      TableName: this.table,
      Item: {
        ...data,
        meta: {
          version: 1,
          createdAt: new Date().toISOString(),
          createdBy: userId
        }
      },
      ConditionExpression: 'attribute_not_exists(id)'
    };
    await this.ddb.put(params).promise();
    return data;
  }

  async save ({ data, userId }) {
    assert.isDefined(data.id);
    assert.isDefined(data.meta);
    const orig = await this.get({ id: data.id });
    assert.equal(orig.meta.version, data.meta.version, `expected version ${orig.meta.version}, got ${data.meta.version}`);
    data = {
      ...data,
      meta: {
        ...orig.meta,
        version: orig.meta.version + 1,
        modifiedBy: userId,
        modifiedAt: new Date().toISOString()
      }
    };
    const params = {
      TableName: this.table,
      Item: data,
      ConditionExpression: 'attribute_exists(id)'
    };
    const result = await this.ddb.put(params).promise();
    return result;
  }

  async delete (key) {
    const params = {
      TableName: this.table,
      Key: key
    };
    await this.ddb.delete(params).promise();
  }

  async query ({ index, expression, filter, values, attrs }) {
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

  async scan ({ projection } = {}) {
    let list = [];
    let params = {
      TableName: this.table
    };
    if (projection) params.ProjectionExpression = projection;
    do {
      const response = await this.ddb.scan(params).promise();
      params.ExclusiveStartKey = response.LastEvaluatedKey;
      list = [...list, ...response.Items];
    } while (params.ExclusiveStartKey);
    return list;
  }

  async update (key, data) {
    let set = [];
    let attrs = {};
    let ean = {};
    for (const name in data) {
      let value = data[name];
      if (value === undefined) continue;
      attrs[`:${name}`] = value;
      ean[`#${name}`] = name;
      if (_.isArray(value)) {
        attrs[':empty_list'] = [];
        set.push(`#${name} = list_append(if_not_exists(#${name}, :empty_list), :${name})`);
      } else {
        set.push(`#${name} = :${name}`);
      }
    }
    const params = {
      TableName: this.table,
      Key: key,
      UpdateExpression: `SET ${set.join(', ')}`,
      ExpressionAttributeValues: attrs,
      ExpressionAttributeNames: ean,
      ReturnValues: 'ALL_NEW'
    };

    const result = await this.ddb.update(params).promise();
    return result.Attributes;
  }

  async patch (key, data) {
    let set = [];
    let attrs = {};
    let names = {};
    for (const name in data) {
      const v = name.split('.');
      attrs[`:variable_${set.length}`] = data[name];
      set.push(`${v.map(i => `#${i}`).join('.')} = :variable_${set.length}`);
      v.forEach(n => { names[`#${n}`] = n; });
    }
    const params = {
      TableName: this.table,
      Key: key,
      UpdateExpression: `SET ${set.join(', ')}`,
      ExpressionAttributeValues: attrs,
      ExpressionAttributeNames: names,
      ReturnValues: 'ALL_NEW'
    };
    console.log(params);
    const result = await this.ddb.update(params).promise();
    return result.Attributes;
  }
}

module.exports = DynamoDB;
