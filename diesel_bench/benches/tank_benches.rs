#![allow(clippy::unused_unit)]

use crate::Bencher;
use std::collections::HashMap;
use tank::{
    Connection, Entity, Executor, QueryBuilder, cols, expr, join,
    stream::{StreamExt, TryStreamExt},
};
use tokio::runtime::Runtime;

#[cfg(feature = "tank-postgres")]
type DbConnection = tank_postgres::PostgresConnection;
#[cfg(feature = "tank-mysql")]
type DbConnection = tank_mysql::MySQLConnection;
#[cfg(feature = "tank-sqlite")]
type DbConnection = tank_sqlite::SQLiteConnection;

#[derive(Entity, Debug, Clone)]
#[tank(name = "users")]
pub struct User {
    #[tank(primary_key)]
    pub id: i32,
    pub name: String,
    pub hair_color: Option<String>,
}

#[derive(Entity, Debug, Clone)]
#[tank(name = "users")]
pub struct NewUser {
    pub name: String,
    pub hair_color: Option<String>,
}

#[derive(Entity, Debug, Clone)]
#[tank(name = "users")]
pub struct UserId {
    #[tank(primary_key)]
    pub id: i32,
}

#[derive(Entity, Debug, Clone)]
#[tank(name = "posts")]
pub struct Post {
    #[tank(primary_key)]
    pub id: i32,
    pub user_id: i32,
    pub title: String,
    pub body: Option<String>,
}

#[derive(Entity, Debug, Clone)]
#[tank(name = "posts")]
pub struct NewPost {
    pub user_id: i32,
    pub title: String,
    pub body: Option<String>,
}

#[derive(Entity, Debug, Clone)]
#[tank(name = "posts")]
pub struct PostId {
    #[tank(primary_key)]
    pub id: i32,
}

#[derive(Entity, Debug, Clone)]
#[tank(name = "comments")]
pub struct Comment {
    #[tank(primary_key)]
    pub id: i32,
    pub post_id: i32,
    pub text: String,
}

#[derive(Entity, Debug, Clone)]
#[tank(name = "comments")]
pub struct NewComment {
    pub post_id: i32,
    pub text: String,
}

#[derive(Entity, Debug)]
#[tank(name = "user_posts")]
struct UserPost {
    #[tank(name = "user_id")]
    u_id: i32,
    #[tank(name = "user_name")]
    u_name: String,
    #[tank(name = "user_hair_color")]
    u_hair_color: Option<String>,
    #[tank(name = "post_id")]
    p_id: Option<i32>,
    #[tank(name = "post_user_id")]
    p_user_id: Option<i32>,
    #[tank(name = "post_title")]
    p_title: Option<String>,
    #[tank(name = "post_body")]
    p_body: Option<String>,
}

#[cfg(feature = "tank-postgres")]
async fn connection() -> DbConnection {
    dotenvy::dotenv().ok();
    let connection_url = dotenvy::var("POSTGRES_DATABASE_URL")
        .or_else(|_| dotenvy::var("DATABASE_URL"))
        .expect("DATABASE_URL must be set in order to run tests");
    let mut conn =
        DbConnection::connect(&tank_postgres::PostgresDriver::new(), connection_url.into())
            .await
            .unwrap();
    cleanup(&mut conn).await;
    conn
}

#[cfg(feature = "tank-mysql")]
async fn connection() -> DbConnection {
    dotenvy::dotenv().ok();
    let connection_url = dotenvy::var("MYSQL_DATABASE_URL")
        .or_else(|_| dotenvy::var("DATABASE_URL"))
        .expect("DATABASE_URL must be set in order to run tests");
    let mut conn = DbConnection::connect(&tank_mysql::MySQLDriver::mysql(), connection_url.into())
        .await
        .unwrap();
    cleanup(&mut conn).await;
    conn
}

#[cfg(feature = "tank-sqlite")]
async fn connection() -> DbConnection {
    let mut conn = DbConnection::connect(
        &tank_sqlite::SQLiteDriver::new(),
        "sqlite://:memory:".into(),
    )
    .await
    .unwrap();
    cleanup(&mut conn).await;
    conn
}

async fn cleanup(conn: &mut DbConnection) {
    User::create_table(conn, true, false).await.unwrap();
    Post::create_table(conn, true, false).await.unwrap();
    Comment::create_table(conn, true, false).await.unwrap();
    Comment::delete_many(conn, true).await.unwrap();
    Post::delete_many(conn, true).await.unwrap();
    User::delete_many(conn, true).await.unwrap();
}

async fn insert_users(
    conn: &mut DbConnection,
    size: usize,
    hair_color_init: impl Fn(usize) -> Option<&'static str> + Sync,
) {
    NewUser::insert_many(
        conn,
        (0..size).map(|i| NewUser {
            name: format!("User {}", i),
            hair_color: hair_color_init(i).map(str::to_string),
        }),
    )
    .await
    .unwrap();
}

pub fn bench_trivial_query(b: &mut Bencher, size: usize) {
    let runtime = Runtime::new().unwrap();
    let (mut conn, mut query) = runtime.block_on(async {
        let mut conn = connection().await;
        insert_users(&mut conn, size, |_| None).await;
        let query = User::prepare_find(&mut conn, true, None).await.unwrap();
        (conn, query)
    });

    b.iter(|| {
        runtime.block_on(async {
            conn.fetch(&mut query)
                .map(|row| User::from_row(row.unwrap()).unwrap())
                .collect::<Vec<User>>()
                .await
        })
    })
}

pub fn bench_medium_complex_query(b: &mut Bencher, size: usize) {
    let runtime = Runtime::new().unwrap();
    let (mut conn, mut query) = runtime.block_on(async {
        let mut conn = connection().await;
        insert_users(&mut conn, size, |i| {
            Some(if i % 2 == 0 { "black" } else { "brown" })
        })
        .await;

        let driver = conn.driver();
        let query = QueryBuilder::new()
            .select(cols!(
                User::id as user_id,
                User::name as user_name,
                User::hair_color as user_hair_color,
                Post::id as post_id,
                Post::user_id as post_user_id,
                Post::title as post_title,
                Post::body as post_body,
            ))
            .from(join!(User LEFT JOIN Post ON User::id == Post::user_id))
            .where_expr(expr!(User::hair_color == "black"))
            .build(&driver);
        let query = conn.prepare(query).await.unwrap();
        (conn, query)
    });

    b.iter(|| {
        runtime.block_on(async {
            conn.fetch(&mut query)
                .map(|row| {
                    let row = UserPost::from_row(row.unwrap()).unwrap();
                    (
                        User {
                            id: row.u_id,
                            name: row.u_name,
                            hair_color: row.u_hair_color,
                        },
                        row.p_id.map(|id| Post {
                            id,
                            user_id: row.p_user_id.unwrap(),
                            title: row.p_title.unwrap(),
                            body: row.p_body,
                        }),
                    )
                })
                .collect::<Vec<(User, Option<Post>)>>()
                .await
        })
    })
}

pub fn bench_insert(b: &mut Bencher, size: usize) {
    let runtime = Runtime::new().unwrap();
    let mut conn = runtime.block_on(connection());

    b.iter(|| {
        runtime.block_on(async {
            NewUser::insert_many(
                &mut conn,
                (0..size).map(|i| NewUser {
                    name: format!("User {}", i),
                    hair_color: Some("hair_color".to_string()),
                }),
            )
            .await
            .unwrap();
        })
    })
}

pub fn loading_associations_sequentially(b: &mut Bencher) {
    #[cfg(feature = "tank-sqlite")]
    const USER_NUMBER: usize = 9;
    #[cfg(not(feature = "tank-sqlite"))]
    const USER_NUMBER: usize = 100;

    let runtime = Runtime::new().unwrap();

    let (mut conn, mut users_query, mut posts_query, mut comments_query) =
        runtime.block_on(async {
            let mut conn = connection().await;

            insert_users(&mut conn, USER_NUMBER, |i| {
                Some(if i % 2 == 0 { "black" } else { "brown" })
            })
            .await;

            let user_ids: Vec<i32> = UserId::find_many(&mut conn, true, None)
                .map_ok(|user| user.id)
                .try_collect()
                .await
                .unwrap();

            NewPost::insert_many(
                &mut conn,
                user_ids.iter().flat_map(|user_id| {
                    (0..10).map(move |i| NewPost {
                        user_id: *user_id,
                        title: format!("Post {} by user {}", i, user_id),
                        body: None,
                    })
                }),
            )
            .await
            .unwrap();

            let post_ids: Vec<i32> = PostId::find_many(&mut conn, true, None)
                .map_ok(|post| post.id)
                .try_collect()
                .await
                .unwrap();

            NewComment::insert_many(
                &mut conn,
                post_ids.iter().flat_map(|post_id| {
                    (0..10).map(move |i| NewComment {
                        post_id: *post_id,
                        text: format!("Comment {} on post {}", i, post_id),
                    })
                }),
            )
            .await
            .unwrap();

            let users_query = User::prepare_find(&mut conn, true, None).await.unwrap();
            let posts_query = conn
                .prepare(
                    QueryBuilder::new()
                        .select(Post::columns())
                        .from(Post::table())
                        .where_expr(expr!(Post::user_id == #user_ids as IN))
                        .build(&conn.driver()),
                )
                .await
                .unwrap();
            let comments_query = conn
                .prepare(
                    QueryBuilder::new()
                        .select(Comment::columns())
                        .from(Comment::table())
                        .where_expr(expr!(Comment::post_id == #post_ids as IN))
                        .build(&conn.driver()),
                )
                .await
                .unwrap();

            (conn, users_query, posts_query, comments_query)
        });

    b.iter(|| {
        runtime.block_on(async {
            let users: Vec<User> = conn
                .fetch(&mut users_query)
                .map(|row| User::from_row(row.unwrap()).unwrap())
                .collect()
                .await;

            let posts: Vec<Post> = conn
                .fetch(&mut posts_query)
                .map(|row| Post::from_row(row.unwrap()).unwrap())
                .collect()
                .await;

            let comments: Vec<Comment> = conn
                .fetch(&mut comments_query)
                .map(|row| Comment::from_row(row.unwrap()).unwrap())
                .collect()
                .await;

            let mut posts_map = posts
                .into_iter()
                .map(|p| (p.id, (p, Vec::new())))
                .collect::<HashMap<_, _>>();

            let mut users_map = users
                .into_iter()
                .map(|u| (u.id, (u, Vec::new())))
                .collect::<HashMap<_, _>>();

            for comment in comments {
                posts_map.get_mut(&comment.post_id).unwrap().1.push(comment);
            }

            for (_, post_with_comments) in posts_map {
                users_map
                    .get_mut(&post_with_comments.0.user_id)
                    .unwrap()
                    .1
                    .push(post_with_comments);
            }

            users_map
                .into_values()
                .collect::<Vec<(User, Vec<(Post, Vec<Comment>)>)>>()
        })
    })
}
