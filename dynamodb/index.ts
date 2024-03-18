import {
  BatchGetItemCommand,
  CreateTableCommand,
  CreateTableCommandInput,
  DescribeTableCommand,
  DynamoDB,
  QueryCommand,
  ScanCommand,
  UpdateItemCommand,
} from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { nanoid } from "nanoid";

const dynamo = new DynamoDB({
  region: "eu-west-1",
  endpoint: "http://127.0.0.1:8888",
});

const TableName = "article_interactions";

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
    TableName: TableName,
    KeySchema: [
      { AttributeName: "articleId", KeyType: "HASH" },
      { AttributeName: "theme", KeyType: "RANGE" },
    ],
    AttributeDefinitions: [
      { AttributeName: "articleId", AttributeType: "S" },
      { AttributeName: "theme", AttributeType: "S" },
      { AttributeName: "interactionCount", AttributeType: "N" },
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 5,
      WriteCapacityUnits: 5,
    },
    GlobalSecondaryIndexes: [
      {
        IndexName: "interactionCountIndex",
        KeySchema: [
          { AttributeName: "articleId", KeyType: "HASH" },
          { AttributeName: "interactionCount", KeyType: "RANGE" },
        ],
        Projection: {
          ProjectionType: "KEYS_ONLY",
        },
        ProvisionedThroughput: {
          ReadCapacityUnits: 5,
          WriteCapacityUnits: 5,
        },
      },
    ],
  });

  console.log({ table: table.Table?.TableName });

  // await fillTableWithArticles();

  await queryAll();

  console.log("-----------------");
  const topArticles = await readTop10ArticlesByInteraction();
  await fetchTopArticlesAttrs(topArticles);
}

async function readTop10ArticlesByInteraction() {
  const query = new QueryCommand({
    TableName: TableName,
    IndexName: "interactionCountIndex",
    KeyConditionExpression: "articleId = :articleId",
    ExpressionAttributeValues: {
      ":articleId": { S: "ZO0S92vE" },
    },
    ScanIndexForward: false,
    Limit: 10,
  });

  const results = await dynamo.send(query);
  console.log({ result: results.Items?.map((i) => unmarshall(i)) });

  return results.Items?.map((i) => unmarshall(i)) as Article[];
}

type Article = {
  articleId: string;
  theme: string;
};

async function fetchTopArticlesAttrs(topArticles: Article[]) {
  const keys = topArticles.map((a) => ({
    articleId: { S: a.articleId },
    theme: { S: a.theme },
  }));
  console.log(keys);
  const batchGetItems = new BatchGetItemCommand({
    RequestItems: {
      [TableName]: {
        Keys: keys,
      },
    },
  });

  const r = await dynamo.send(batchGetItems);
  console.log({
    result: r.Responses?.article_interactions.map((i) => unmarshall(i)),
    consumedCapacity: r.ConsumedCapacity,
  });
}

async function fillTableWithArticles() {
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
      TableName: TableName,
      Key: {
        articleId: { S: id },
        theme: { S: theme },
      },
      UpdateExpression:
        "ADD thumbsUp :thumbsUp, thumbsDown :thumbsDown, neutral :neutral, interactionCount :interactionCount SET themeName = :themeName",
      ExpressionAttributeValues: {
        ":themeName": { S: theme },
        ":interactionCount": {
          N: (thumbsUp + thumbsDown + neutral).toString(),
        },
        ":thumbsUp": { N: thumbsUp.toString() },
        ":thumbsDown": { N: thumbsDown.toString() },
        ":neutral": { N: neutral.toString() },
      },
      ReturnValues: "ALL_NEW",
    });
    return item;
  });

  await dynamo.send(articles[0]);

  for (const articleCommand of articles) {
    await dynamo.send(articleCommand);
  }
}

async function queryAll() {
  // Query all
  const command = new ScanCommand({
    TableName: TableName,
    Limit: 100,
  });

  const result = await dynamo.send(command);
  console.log({ result: result.Items?.map((i) => unmarshall(i)) });
}

async function queryByArticleId(articleId: string) {
  // Query all themes for an article
  const queryCmd = new QueryCommand({
    TableName: TableName,
    KeyConditionExpression: "articleId = :articleId",
    ExpressionAttributeValues: {
      // Use an existing id
      ":articleId": { S: articleId },
    },
  });

  const r = await dynamo.send(queryCmd);
  console.log({ result: r.Items?.map((i) => unmarshall(i)) });
}

Test();
