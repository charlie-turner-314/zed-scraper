import fetch from "node-fetch";
import { MongoClient } from "mongodb";
import pkg, { gql } from "graphql-request";
import { GraphQLClient } from "graphql-request";

const raceendpoint = `https://zed-ql.zed.run/graphql`;
const token = `eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJjcnlwdG9maWVsZF9hcGkiLCJleHAiOjE2MzQwMDQ3MDksImlhdCI6MTYzMTU4NTUwOSwiaXNzIjoiY3J5cHRvZmllbGRfYXBpIiwianRpIjoiODAzNmIxZTQtNjA1My00MzQ3LTg3ODctNTExN2JlOWUyMzkwIiwibmJmIjoxNjMxNTg1NTA4LCJzdWIiOnsiZXh0ZXJuYWxfaWQiOiI3YjQ2MWY5ZC1iM2M3LTQyYjgtYThkNS0zMjU3Nzg4OGZjN2YiLCJpZCI6MTYzMTc4LCJwdWJsaWNfYWRkcmVzcyI6IjB4MGRCMjBCRWVmNUY1NWIxMTc4NzY3YmM4NDgwNEUzMGZFYjk4M2RlMSIsInN0YWJsZV9uYW1lIjoiUmFjZWhvcnNlcyBBbm9ueW1vdXMifSwidHlwIjoiYWNjZXNzIn0._kRuTXigI7auxrXKBWPNVaDrHZRBSX1tEKu-idLmEU5Tdlgjuia4H3Hiyh9TQAmYsVrouYm0pqCQycCLeC6IwQ`;
const connectionURI =
  "mongodb+srv://charlieturner:Smoothbox678@zeddata.rk84a.mongodb.net/zed?retryWrites=true&w=majority";
// 1 - Horses, 2 - Races
const option = 2;

const main = async () => {
  const mongoClient = new MongoClient(connectionURI);
  try {
    await mongoClient.connect();
    switch (option) {
      case 1: {
        let offset = 0;
        while (offset === 0) {
          const res = await fetchHorses(mongoClient, offset);
          if (res === -1) break;
          offset += res;
        }
        await fetchHorses(mongoClient);
        break;
      }
      case 2: {
        const graphqlClient = new GraphQLClient(raceendpoint, {
          headers: {
            "x-developer-secret": token,
            "Content-Type": "application/json",
          },
        });
        let cursor = 0;
        while (true) {
          const res = await fetchRaces(mongoClient, graphqlClient, cursor);
          if (res === -1) break;
          cursor = res;
        }
        break;
      }
      default: {
        console.error("Not a valid option");
        break;
      }
    }
  } catch (e) {
    console.error(await e);
  } finally {
    console.log("Closing MongoDB Client...");
    mongoClient.close();
  }
};

const listDatabases = async (client) => {
  const databaseList = await client.db().admin().listDatabases();
  console.log("Databases:");
  databaseList.databases.forEach((db) => console.log(` - ${db.name}`));
};

// list of horses so that we can insert more than the 10 returned at a time
const fetchHorses = async (client, offset = 0) => {
  let horseList = [];
  const data = await Promise.all(
    [0].map((id) =>
      fetch(
        `https://api.zed.run/api/v1/horses/roster?offset=${offset + 10 * id}`
      ).then((res) => res.json())
    )
  );
  const flatData = [].concat(...data);
  if (flatData.length < 1) {
    console.log("No more horses");
    return -1;
  }

  const horses = flatData.map((horse) => {
    return {
      horse_id: horse.horse_id,
      name: horse.hash_info.name,
      bloodline: horse.bloodline,
      breed_type: horse.breed_type,
      coat: horse.hash_info.color,
      genotype: horse.genotype,
      gender: horse.horse_type,
      img_url: horse.img_url,
      owner: horse.owner,
      super_coat: horse.super_coat,
    };
  });
  horseList.push(...horses);

  try {
    await client
      .db("zed")
      .collection("horses")
      .insertMany(horseList, { ordered: false });
    console.log(`Inserting ${horseList.length} horses`);
    return horseList.length;
  } catch (e) {
    console.error(e);
    return -1;
  }
};

const fetchRaces = async (mongoClient, graphqlClient, cursor) => {
  const after = cursor ? cursor : null;
  const query = gql`
    query (
      $input: GetRaceResultsInput
      $before: String
      $after: String
      $first: Int
      $last: Int
    ) {
      get_race_results(
        before: $before
        after: $after
        first: $first
        last: $last
        input: $input
      ) {
        edges {
          node {
            race_id
            name
            start_time
            class
            length
            country
            city
            weather
            fee
            prize_pool {
              first
              second
              third
            }
            horses {
              horse_id
              gate
              final_position
              finish_time
            }
          }
        }
        page_info {
          end_cursor
          has_next_page
        }
      }
    }
  `;

  const variables = {
    first: 2000,
    after: after,
    input: {
      distance: {
        from: 1000,
        to: 2600,
      },
    },
  };
  let raceData, pageInfo;
  try {
    const data = await graphqlClient.request(query, variables);
    raceData = await data.get_race_results.edges;
    pageInfo = await data.get_race_results.page_info;
  } catch (e) {
    console.error(e);
    return -1;
  }

  const races = [];
  const horseResults = [];
  raceData.forEach((edge) => {
    const race = edge.node;
    const horses = race.horses;
    races.push({
      race_id: race.race_id,
      race_name: race.name,
      country: race.country,
      city: race.city,
      class: race.class,
      distance: race.length,
      time: new Date(race.start_time).getTime(),
      weather: race.weather,
      prizepool: {
        first: race.prize_pool.first * 10 ** -18,
        second: race.prize_pool.third * 10 ** -18,
        third: race.prize_pool.third * 10 ** -18,
      },
    });
    horseResults.push(
      ...horses.map((horse) => {
        let prize = 0;
        switch (horse.final_position) {
          case "1": {
            prize = race.prize_pool.first * 10 ** -18;
            break;
          }
          case "2": {
            prize = race.prize_pool.second * 10 ** -18;
            break;
          }
          case "3": {
            prize = race.prize_pool.third * 10 ** -18;
            break;
          }
        }
        return {
          race_id: race.race_id,
          horse_id: horse.horse_id,
          position: horse.final_position,
          time: horse.finish_time,
          gate: horse.gate,
          fee: race.fee,
          prize: prize,
        };
      })
    );
  });
  try {
    await mongoClient
      .db("zed")
      .collection("races")
      .insertMany(races, { ordered: false });
    console.log(`Inserting ${races.length} Races`);
  } catch (e) {
    console.error(e);
  }
  try {
    await mongoClient
      .db("zed")
      .collection("race_results")
      .insertMany(horseResults, { ordered: false });
    console.log(`Inserting ${horseResults.length} Race Results`);
  } catch (e) {
    console.error(e);
  }
  if (pageInfo.has_next_page) {
    return pageInfo.end_cursor;
  } else {
    return -1;
  }
};

main();
