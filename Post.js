const { Client } = require('pg');
const axios = require('axios');
const qs = require('qs');
const _max = require('lodash/max');
const _min = require('lodash/min');
const dotenv = require('dotenv');

dotenv.config();
let pg;
let task;
let fbData;
let cleanData;
let alreadyExists;
let edges;
let final;

async function say(message, error, exit) {
  if (error) {
    console.error(`${message}`);
  } else {
    console.log(`${message}`);
  }

  if (error || exit) {
    await say('Disconnecting DB..');
    await pg.query(`SELECT pg_advisory_unlock(1);`);
    await pg.end();
    process.exit(1);
  }
}

async function main() {
  // STEP: Connect to postgres db
  {
    pg = new Client();
    try {
      await say('Connecting to postgres db');
      await pg.connect();
      await say('Connected');
    } catch (err) {
      await say(err.message, true);
    }

  }

  // STEP: Crete tables if not created
  {
    await say('Creating page_list table if needed');
    try {
      await pg.query(`CREATE TABLE IF NOT EXISTS "page_list" (
                                    fb_account_id varchar(100) primary key,
                                    finished bool,
                                    cursor_timestamp bigint default null,
                                    next_try_date timestamptz default now()
                                    );`);
      await say('Created');
    } catch (err) {
      await say(err.message, true);
    }

    await say('Creating posts table if needed');
    try {
      await pg.query(`CREATE TABLE IF NOT EXISTS "post_list" (
                                    feedback_id varchar(200) primary key,
                                    fb_account_id varchar(200) not null,
                                    post_id varchar(200) not null,
                                    date bigint,
                                    url text,
                                    text text,
                                    story_id text
                                    );`);
      await say('Created');
    } catch (err) {
      await say(err.message, true);
    }

  }

  // STEP: Find a task to do
  {
    await say('Trying to get advisory lock for page_list');
    try {
      await pg.query(`SELECT pg_advisory_lock(1);`);
    } catch (err) {
      await say(err.message, true);
    }

    await say('Trying to find a task');
    try {
      const result = await pg.query(`SELECT * FROM page_list WHERE next_try_date < now() LIMIT 1;`);
      task = result.rows?.[0];
      if (!task) {
        await say('No tasks available', undefined, true);
      }

      await say(`Increasing date of the task: ${task.fb_account_id}`);
      await pg.query(`UPDATE page_list SET next_try_date=(SELECT current_timestamp + interval '15 seconds') WHERE fb_account_id='${task.fb_account_id}';`);
    } catch (err) {
      await say(err.message, true);
    }

    await say('Unlocking advisory lock for page_list');
    try {
      await pg.query(`SELECT pg_advisory_unlock(1);`);
    } catch (err) {
      await say(err.message, true);
    }
  }

  // STEP: Get data from FB
  {
    const afterTime = task.finished ? (task.cursor_timestamp ? Number(task.cursor_timestamp) + 1 : null) : null;
    const beforeTime = task.finished ? null : (task.cursor_timestamp ? Number(task.cursor_timestamp) - 1 : null);
    const data = qs.stringify({
      'fb_api_caller_class': 'RelayModern',
      'fb_api_req_friendly_name': 'ProfileCometTimelineFeedRefetchQuery',
      'server_timestamps': 'true',
      'doc_id': process.env.DOC_ID,
      variables: JSON.stringify({
        "UFI2CommentsProvider_commentsKey": "ProfileCometTimelineRoute",
        "beforeTime": beforeTime,
        "afterTime": afterTime,
        "count": 3,
        "cursor": null,
        "displayCommentsContextEnableComment": null,
        "displayCommentsContextIsAdPreview": null,
        "displayCommentsContextIsAggregatedShare": null,
        "displayCommentsContextIsStorySet": null,
        "displayCommentsFeedbackContext": null,
        "feedLocation": "TIMELINE",
        "feedbackSource": 0,
        "focusCommentID": null,
        "memorializedSplitTimeFilter": null,
        "omitPinnedPost": true,
        "postedBy": null,
        "privacy": null,
        "privacySelectorRenderLocation": "COMET_STREAM",
        "renderLocation": "timeline",
        "scale": 1,
        "should_show_profile_pinned_post": true,
        "stream_count": 3,
        "taggedInOnly": null,
        "useDefaultActor": false,
        "id": task.fb_account_id
      })
    });
    const config = {
      method: 'post',
      url: 'https://www.facebook.com/api/graphql/',
      headers: {
        'authority': 'www.facebook.com',
        'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
        'viewport-width': '1040',
        'x-fb-friendly-name': 'ProfileCometTimelineFeedRefetchQuery',
        'content-type': 'application/x-www-form-urlencoded',
        'sec-ch-prefers-color-scheme': 'light',
        'sec-ch-ua-platform': '"macOS"',
        'accept': '*/*',
        'origin': 'https://www.facebook.com',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty'
      },
      data : data
    };

    try {
      await say('Making a request to facebook');
      const response = await axios(config);
      fbData = response.data;

      if (!fbData) {
        throw new Error('Fb response is empty');
      }
      fbData = typeof fbData === 'string' ? fbData : JSON.stringify(fbData);
      await say('Data received');
    } catch (err) {
      await say(err.message, true);
    }
  }

  // STEP: Clean dirty data
  {
    await say('Cleaning data');
    const splited = fbData.replaceAll('for (;;);', '').split(/}\r*\n*{/);
    const cleaned = splited
      .map((item, index) => {
        if (splited.length === 1) return item;
        if (index === 0 ) return item + '}';
        if (index === splited.length - 1) return '{' + item;
        return '{' + item + '}';
      });

    try {
      cleanData = JSON.parse(cleaned[0]);

      if (!cleanData?.data) {
        throw new Error();
      }
    } catch (e) {
      await say('Cannot clean the data', true)
    }
  }

  // STEP: Exit if finished
  {
    final = fbData.includes('{"is_final":true}') && fbData.includes('ProfileCometTimelineFeed') && fbData.includes('"end_cursor":null');
    if (final) {
      await say('Trying to find the latest post');
      const result = await pg.query(`SELECT * from post_list where fb_account_id='${task.fb_account_id}' ORDER BY date DESC LIMIT 1;`);
      const theLastPost = result?.rows?.[0];
      if (!theLastPost) {
        await say(`The last post is not fount for account ${task.fb_account_id}`, true);
      }

      await say('No posts found. Final post scraped');
      await pg.query(`UPDATE page_list set next_try_date=now()::DATE + 1, cursor_timestamp=${theLastPost.date}, finished=True, WHERE fb_account_id='${task.fb_account_id}';`);
      await say('The task is postponed', undefined, true);
    }
  }

  // STEP: Saving posts
  {
    edges = (cleanData?.data?.node?.timeline_list_feed_units?.edges || [])
      .filter((item) =>
        item.node?.comet_sections?.content?.story?.actors?.find((a) => a.id === task.fb_account_id) !== -1 &&
        item.node?.comet_sections?.context_layout?.story?.comet_sections?.metadata?.[0]?.story?.creation_time);
    const rows = edges.map(({ node }) => {
      const buff = Buffer.from(node.feedback?.id || '', 'base64');
      const postId = buff.toString().match(/\d+/)?.[0];
      const dateRaw = node.comet_sections?.context_layout?.story?.comet_sections?.metadata?.[0]?.story?.creation_time;
      const url = node.comet_sections?.content?.story?.wwwURL;
      const text = node.comet_sections?.content?.story?.comet_sections?.message?.story?.message?.text;
      const storyId = node.comet_sections?.content?.story?.id;

      return [
        node.feedback?.id,
        task.fb_account_id,
        postId,
        dateRaw,
        url,
        text,
        storyId
      ];
    });

    for (let row of rows) {
      try {
        if (!row[0]) {
          await say('No feedback_id found')
          continue;
        }
        await say('Saving post into DB');

        await pg.query(`INSERT INTO "post_list" ("feedback_id", "fb_account_id", "post_id", "date", "url", "text", "story_id") VALUES ($1, $2, $3, $4, $5, $6, $7)`, row);
        await say(`Post saved (${row[4]})`);
        await say(`[${new Date(row[3] * 1000).toLocaleDateString()}] ${row[5]?.slice(0, 30)}`)
      } catch (err) {
        if (err.detail?.includes('feedback_id') && err.detail?.includes('already exists')) {
          alreadyExists = true;
          await say(`Post(${row[4]}) already exists for account ${task.fb_account_id}`);
          await say(`[${new Date(row[3] * 1000).toLocaleDateString()}] ${row[5]?.slice(0, 30)}`)
        } else {
          await say(err.message, true)
        }
      }
    }

    const timestamps = edges.map(({ node }) => {
      return node.comet_sections?.context_layout?.story?.comet_sections?.metadata?.[0]?.story?.creation_time;
    });
    const cursorTimestamp = task.finished ? _max(timestamps) : _min(timestamps);
    await say(`Updating cursor_timestamp to ${cursorTimestamp}`);
    await pg.query(`UPDATE page_list set cursor_timestamp=${cursorTimestamp} WHERE fb_account_id='${task.fb_account_id}';`);
  }

  // STEP: Restarting
  {
    await say('Exiting', undefined, true);
  }
}

function startMain() {
  say('Job will be started in 1 sec')
  setTimeout(() => {
    main();
  }, 1000)
}

startMain();
