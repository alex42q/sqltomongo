/*
    - Created By alex42q -
Convert postgres or .sql file to mongodb Migration
*/

const fs = require("fs");
const mongoose = require("mongoose");

const args = process.argv.slice(2); // Skip the first two default entries
const databaseUrl = args[0]; // First user argument: MongoDB URL
const filePath = args[1]; // Second user argument: File path to read SQL data

const connectToMongo = async (url) => {
  try {
    await mongoose.connect(url, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
    console.log("Connected to MongoDB successfully");
  } catch (err) {
    console.error("Failed to connect to MongoDB", err);
    process.exit(1);
  }
};

const sleep = (time) => {
  return new Promise((resolve) => setTimeout(resolve, time));
};

function inferDataTypes(sql) {
  const firstParenthesisIndex = sql.indexOf("(");
  const firstParenthesisCloseIndex = sql.indexOf(")", firstParenthesisIndex);
  const columnNames = sql
    .substring(firstParenthesisIndex + 1, firstParenthesisCloseIndex)
    .split(",")
    .map((name) => name.trim().replace(/['" ]/g, ""));

  const valuesStartIndex =
    sql.indexOf("VALUES", firstParenthesisCloseIndex) + "VALUES".length;
  const firstRowEndIndex = sql.indexOf(")", valuesStartIndex);
  const values = sql
    .substring(valuesStartIndex, firstRowEndIndex)
    .split(",")
    .map((value) => value.trim().replace(/^['"]|['"]$/g, ""));

  const types = values.map((value) => {
    if (/^\d{4}-\d{2}-\d{2}/.test(value)) return { type: Date, default: null }; // Allow nulls for dates
    if (value === "NULL")
      return { type: mongoose.Schema.Types.Mixed, default: null }; // General handling for 'NULL' as null
    if (/^\d+$/.test(value)) return { type: Number };
    if (/^\d+\.\d+$/.test(value)) return { type: Number };
    if (/^true$|^false$/i.test(value)) return { type: Boolean };
    return { type: String };
  });

  const schema = {};
  columnNames.forEach((name, i) => {
    schema[name] = types[i];
  });

  return schema;
}

const startProgram = async () => {
  await connectToMongo(databaseUrl);

  fs.readFile(filePath, "utf8", async (err, data) => {
    if (err) {
      console.error("Error reading file:", err);
      return;
    }

    console.log(inferDataTypes(data));

    try {
      //Split columns
      const startIndex = data.indexOf("INSERT INTO public.");
      const endIndex = data.indexOf(")", startIndex);
      const insertSection = data.substring(startIndex, endIndex);

      const tableInfo = insertSection
        .replace("INSERT INTO public.", "")
        .split("(")[1]
        .replace(/['"]/g, "");

      const columns = tableInfo
        .split(",")
        .map((item) => item.trim())
        .filter((item) => item);
      console.log(data.split('"')[1]);
      const Schema = new mongoose.Schema(inferDataTypes(data));
      const TheeModel = mongoose.model(data.split('"')[1], Schema);
      await TheeModel.createCollection();
      //Split columns
      await sleep(1000);
      //Split data
      const dataStartIndex = data.indexOf("VALUES");
      const splitData = data.slice(dataStartIndex).replace("VALUES", "").trim();
      const rowRegex = /\(([^()]+)\)/g;
      let match;
      const rows = [];

      while ((match = rowRegex.exec(splitData))) {
        rows.push(match[1]);
      }

      const rowsWithColumns = rows.map((row) => {
        console.log(row);
        return row
          .match(/('[^']*'|[^,]+)/g)
          .map((field) => field.replace(/^'(.*)'$/, "$1"));
      });

      //Split data

      //Combine data with columns
      for (const row of rowsWithColumns) {
        try {
          let rowObject = {};
          console.log("\n");
          for (const [index, value] of row.entries()) {
            const key = columns[index];
            rowObject[key] = value;

            console.log(`${key}: ${value}`);
          }
          console.log(rowObject);
          await TheeModel.create(rowObject);
          console.log("\n"); // Processed row object
          await sleep(3000); // Sleep for 1 second
        } catch (ex) {
          console.error("Failed to insert data:", ex.message);
        }
      }
    } catch (ex) {
      console.error("Failed to parse SQL data:", ex.message);
    } finally {
      mongoose.connection.close();
    }
  });
};

startProgram();
