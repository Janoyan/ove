const { Client } = require('pg');
const axios = require('axios');
const qs = require('qs');
const dotenv = require('dotenv');

dotenv.config();
let pg;
let task;
let fbData;
let cleanData;
let alreadyExists;
let edges;
let final;

function say(message, error, exit) {
  if (error) {
    console.error(`${message}`);
  } else {
    console.log(`${message}`);
  }

  if (error || exit) {
    process.exit(1);
  }
}

async function main() {
  // STEP: Connect to postgres db
  {
    pg = new Client();
    try {
      say('Connecting to postgres db');
      await pg.connect();
      say('Connected');
    } catch (err) {
      say(err.message, true);
    }

  }

  // STEP: Crete tables if not created
  {
    say('Creating page_list table if needed');
    try {
      await pg.query(`CREATE TABLE IF NOT EXISTS "page_list" (
                                    fb_account_id varchar(100) primary key,
                                    finished bool,
                                    cursor text default null,
                                    next_try_date timestamptz default now()
                                    );`);
      say('Created');
    } catch (err) {
      say(err.message, true);
    }

    say('Creating posts table if needed');
    try {
      await pg.query(`CREATE TABLE IF NOT EXISTS "post_list" (
                                    feedback_id varchar(200) primary key,
                                    fb_account_id varchar(200) not null,
                                    post_id varchar(200) not null,
                                    date timestamptz,
                                    url text,
                                    text text,
                                    story_id text
                                    );`);
      say('Created');
    } catch (err) {
      say(err.message, true);
    }

  }

  // STEP: Find a task to do
  {
    say('Trying to get advisory lock for page_list');
    try {
      await pg.query(`SELECT pg_advisory_lock(1);`);
    } catch (err) {
      say(err.message, true);
    }

    say('Trying to find a task');
    try {
      const result = await pg.query(`SELECT * FROM page_list WHERE next_try_date < now() LIMIT 1;`);
      task = result.rows?.[0];
      if (!task) {
        say('No tasks available', undefined, true);
      }

      say(`Increasing date of the task: ${task.fb_account_id}`);
      await pg.query(`UPDATE page_list SET next_try_date=(SELECT current_timestamp + interval '30 seconds') WHERE fb_account_id='${task.fb_account_id}';`);
    } catch (err) {
      say(err.message, true);
    }

    say('Unlocking advisory lock for page_list');
    try {
      await pg.query(`SELECT pg_advisory_unlock(1);`);
    } catch (err) {
      say(err.message, true);
    }
  }

  // STEP: Get data from FB
  {
    const data = qs.stringify({
      'fb_api_caller_class': 'RelayModern',
      'fb_api_req_friendly_name': 'ProfileCometTimelineFeedRefetchQuery',
      'server_timestamps': 'true',
      'doc_id': process.env.DOC_ID,
      variables: JSON.stringify({
        "UFI2CommentsProvider_commentsKey": "ProfileCometTimelineRoute",
        "afterTime": null,
        "beforeTime": null,
        "count": 3,
        "cursor": task.cursor,
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
        "stream_count": 1,
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
      say('Making a request to facebook');
      const response = await axios(config);
      fbData = response.data;

      if (!fbData) {
        throw new Error('Fb response is empty');
      }
      fbData = typeof fbData === 'string' ? fbData : JSON.stringify(fbData);
      say('Data received');
    } catch (err) {
      say(err.message, true);
    }
  }

  // STEP: Clean dirty data
  {
    say('Cleaning data');
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
      say('Cannot clean the data', true)
    }
  }

  // STEP: Exit if finished or empty
  {
    final = fbData.includes('{"is_final":true}') && fbData.includes('ProfileCometTimelineFeed') && fbData.includes('"end_cursor":null');
    if (final) {
      say('No posts found. Final post scraped');
      await pg.query(`UPDATE page_list set next_try_date=now()::DATE + 1, finished=True, cursor=null, WHERE fb_account_id='${task.fb_account_id}';`);
      say('The task is postponed', undefined, true);
    }
  }

  // STEP: Saving posts
  {
    edges = cleanData?.data?.node?.timeline_list_feed_units?.edges || [];
    const rows = edges.map(({ node }) => {
      const buff = Buffer.from(node.feedback?.id, 'base64');
      const postId = buff.toString().match(/\d+/)?.[0];
      const dateRaw = node.comet_sections?.context_layout?.story?.comet_sections?.metadata?.[0]?.story?.creation_time;
      const url = node.comet_sections?.content?.story?.wwwURL;
      const text = node.comet_sections?.content?.story?.comet_sections?.message?.story?.message?.text;
      const storyId = node.comet_sections?.content?.story?.id;

      return [
        node.feedback?.id,
        task.fb_account_id,
        postId,
        dateRaw ? new Date(dateRaw * 1000) : null,
        url,
        text,
        storyId
      ];
    });

    for (let row of rows) {
      try {
        say('Saving post into DB');

        await pg.query(`INSERT INTO "post_list" ("feedback_id", "fb_account_id", "post_id", "date", "url", "text", "story_id") VALUES ($1, $2, $3, $4, $5, $6, $7)`, row);
        say(`Post saved (${row[4]})`);
      } catch (err) {
        if (err.detail?.includes('feedback_id') && err.detail?.includes('already exists')) {
          alreadyExists = true;
          say(`Post(${row[4]}) already exists for account ${task.fb_account_id}`);
        } else {
          say(err.message, true)
        }
      }
    }
  }

  // STEP: Postpone page scraping if post already exists
  {
    if (task.finished && alreadyExists) {
      say('Post already exists. Reseting cursor and postponing the page scraping');
      await pg.query(`UPDATE page_list set next_try_date=now()::DATE + 1, cursor=null WHERE fb_account_id='${task.fb_account_id}';`);
      say('The page task is postponed', undefined, true);
    }
  }

  // STEP: Store page info
  {
    const cursor = edges.length > 0 ? edges[edges.length - 1].cursor : null;
    say(`Setting a new cursor`);
    await pg.query(`UPDATE page_list set cursor='${cursor}' WHERE fb_account_id='${task.fb_account_id}';`);
  }

  // STEP: Restarting
  {
    say('Exiting', undefined, true);
  }
}

function startMain() {
  say('Job will be started in 5 sec')
  setTimeout(() => {
    main();
  }, 5000)
}

startMain();
