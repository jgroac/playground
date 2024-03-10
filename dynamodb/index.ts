import {
  CreateTableCommand,
  CreateTableCommandInput,
  DescribeTableCommand,
  DynamoDB,
  ScanCommand,
  UpdateItemCommand,
} from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { nanoid } from "nanoid";

const dynamo = new DynamoDB({
  region: "eu-west-1",
  endpoint: "http://127.0.0.1:8888",
});

const createTableIfMissing = async (schema: CreateTableCommandInput) => {
  const describeTable = new DescribeTableCommand({
    TableName: schema.TableName,
  });
  let table = await dynamo.send(describeTable);

  if (table) {
    return table;
  }

  table = await dynamo.createTable(schema);

  return table;
};

async function Test() {
  const table = await createTableIfMissing({
    TableName: "article_interactions",
    KeySchema: [
      { AttributeName: "articleId", KeyType: "HASH" },
      { AttributeName: "themeCount", KeyType: "RANGE" },
    ],
    AttributeDefinitions: [
      { AttributeName: "articleId", AttributeType: "S" },
      { AttributeName: "themeCount", AttributeType: "N" },
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 5,
      WriteCapacityUnits: 5,
    },
  });

  const uuid = nanoid(8);
  console.log({ table: table.Table?.TableName, uuid });

  const updateCmd = new UpdateItemCommand({
    TableName: "article_interactions",
    Key: {
      articleId: { S: "Im1H1w20" },
      themeCount: { S: "10" },
    },
    UpdateExpression:
      "ADD thumbsUp :thumbsUp, thumbsDown :thumbsDown, neutral :neutral SET theme = :theme",
    ExpressionAttributeValues: {
      ":thumbsUp": { N: "5" },
      ":thumbsDown": { N: "0" },
      ":neutral": { N: "0" },
      ":theme": { S: "science fiction" },
    },
    ReturnValues: "ALL_NEW",
  });

  const updateResult = await dynamo.send(updateCmd);

  console.log({ updateResult: updateResult.Attributes });

  const command = new ScanCommand({
    TableName: "article_interactions",
    Limit: 10,
  });

  const result = await dynamo.send(command);
  console.log({ result: result.Items?.map((i) => unmarshall(i)) });
}

Test();
