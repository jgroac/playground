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

  try {
    const table = await dynamo.send(describeTable);
    return table;
  } catch (err) {
    const table = await dynamo.createTable(schema);
    return table;
  }
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
      { AttributeName: "themeCount", AttributeType: "S" },
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 5,
      WriteCapacityUnits: 5,
    },
  });

  const uuid = nanoid(8);
  console.log({ table: table.Table?.TableName, uuid });

  const ids = [nanoid(8), nanoid(8), nanoid(8), nanoid(8), nanoid(8)];
  const themes = ["science", "science_fiction", "tech", "twitch", "AI"];
  const articles = Array.from(new Array(300)).map(() => {
    const random = () => Math.round(Math.random() * 4);
    const id = ids[random()];
    const theme = themes[random()];
    if (!id || !theme) {
      throw new Error(`id or theme undefined ${id}, ${theme}, ${random}`);
    }
    const thumbsUp = Math.round(Math.random() * 50);
    const thumbsDown = Math.round(Math.random() * 25);
    const neutral = Math.round(Math.random() * 100);

    const item = new UpdateItemCommand({
      TableName: "article_interactions",
      Key: {
        articleId: { S: id },
        themeCount: { S: theme },
      },
      UpdateExpression:
        "ADD thumbsUp :thumbsUp, thumbsDown :thumbsDown, neutral :neutral",
      ExpressionAttributeValues: {
        ":thumbsUp": { N: thumbsUp.toString() },
        ":thumbsDown": { N: thumbsDown.toString() },
        ":neutral": { N: neutral.toString() },
      },
      ReturnValues: "ALL_NEW",
    });
    return item;
  });

  // await dynamo.send(articles[0]);

  // for (const articleCommand of articles) {
  //   await dynamo.send(articleCommand);
  // }

  const command = new ScanCommand({
    TableName: "article_interactions",
    Limit: 100,
  });

  const result = await dynamo.send(command);
  console.log({ result: result.Items?.map((i) => unmarshall(i)) });
}

Test();
