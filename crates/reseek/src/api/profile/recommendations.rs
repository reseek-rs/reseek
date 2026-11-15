//! What the server makes of interests: what it recommends to us from ours, what is popular
//! across the network, what goes with one interest, and the users whose interests are like
//! ours or who like one. Each is asked of the server when read; any role may read them.

use api_types::profile::{Recommendation, Recommendations, SimilarUser};
use axum::extract::{Path, State};
use soulseek_client::ClientError;
use soulseek_client::soulseek_proto;
use tracing::debug;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::api::error::ApiError;
use crate::api::extract::Json;
use crate::app::AppState;
use crate::auth::{AnyRole, Authorized};

/// The recommendation routes.
pub fn routes() -> OpenApiRouter<AppState> {
  OpenApiRouter::new()
    .routes(routes!(personal))
    .routes(routes!(popular))
    .routes(routes!(similar_users))
    .routes(routes!(related))
    .routes(routes!(liked_by))
}

/// The answer to asking the server for `what` that failed with `error`.
fn unanswered(what: &str, error: ClientError) -> ApiError {
  debug!(what, error = %error, "server did not answer an interests query");
  match error {
    ClientError::InvalidOperation(detail) => ApiError::unavailable(Some(detail)),
    other => ApiError::upstream(
      format!("The server did not send {what}"),
      Some(crate::network::error_text(&other)),
    ),
  }
}

/// The server's lists as the API shows them, [`sorted`].
fn shown(lists: soulseek_proto::server::Recommendations) -> Recommendations {
  let lists = sorted(lists);
  let each = |list: Vec<soulseek_proto::server::Recommendation>| {
    list
      .into_iter()
      .map(|r| Recommendation {
        interest: r.interest,
        score: r.count,
      })
      .collect()
  };
  Recommendations {
    recommended: each(lists.recommendations),
    unrecommended: each(lists.unrecommendations),
  }
}

/// Both blocks of a reply merged and split again by score: above zero is recommended, highest
/// first; zero and below is not, lowest first.
fn sorted(
  recommendations: soulseek_proto::server::Recommendations,
) -> soulseek_proto::server::Recommendations {
  let (mut recommended, mut unrecommended): (Vec<_>, Vec<_>) = recommendations
    .recommendations
    .into_iter()
    .chain(recommendations.unrecommendations)
    .partition(|recommendation| recommendation.count > 0);
  recommended.sort_by_key(|recommendation| std::cmp::Reverse(recommendation.count));
  unrecommended.sort_by_key(|recommendation| recommendation.count);
  soulseek_proto::server::Recommendations {
    recommendations: recommended,
    unrecommendations: unrecommended,
  }
}

/// `interest` as the server keeps interests.
fn interest_of(text: &str) -> Result<String, ApiError> {
  let interest = text.trim().to_lowercase();
  if interest.is_empty() {
    return Err(ApiError::bad_request("An interest may not be blank"));
  }
  Ok(interest)
}

/// Interests the server recommends to us from what we like and hate.
#[utoipa::path(
  get, path = "/interests/recommendations", tag = "profile",
  responses(
    (status = 200, body = Recommendations), (status = 401, body = api_types::ErrorBody),
    (status = 502, body = api_types::ErrorBody), (status = 503, body = api_types::ErrorBody)
  )
)]
async fn personal(
  State(app): State<AppState>,
  _caller: Authorized<AnyRole>,
) -> Result<Json<Recommendations>, ApiError> {
  let lists = app
    .network
    .client()
    .get_personal_recommendations()
    .await
    .map_err(|e| unanswered("recommendations", e))?;
  Ok(Json(shown(lists)))
}

/// Interests popular across the whole network.
#[utoipa::path(
  get, path = "/interests/popular", tag = "profile",
  responses(
    (status = 200, body = Recommendations), (status = 401, body = api_types::ErrorBody),
    (status = 502, body = api_types::ErrorBody), (status = 503, body = api_types::ErrorBody)
  )
)]
async fn popular(
  State(app): State<AppState>,
  _caller: Authorized<AnyRole>,
) -> Result<Json<Recommendations>, ApiError> {
  let lists = app
    .network
    .client()
    .get_global_recommendations()
    .await
    .map_err(|e| unanswered("popular interests", e))?;
  Ok(Json(shown(lists)))
}

/// Users whose interests are like ours, most alike first.
#[utoipa::path(
  get, path = "/interests/similar-users", tag = "profile",
  responses(
    (status = 200, body = Vec<SimilarUser>), (status = 401, body = api_types::ErrorBody),
    (status = 502, body = api_types::ErrorBody), (status = 503, body = api_types::ErrorBody)
  )
)]
async fn similar_users(
  State(app): State<AppState>,
  _caller: Authorized<AnyRole>,
) -> Result<Json<Vec<SimilarUser>>, ApiError> {
  let mut users: Vec<SimilarUser> = app
    .network
    .client()
    .get_similar_users()
    .await
    .map_err(|e| unanswered("similar users", e))?
    .into_iter()
    .map(|user| SimilarUser {
      username: user.username,
      rating: user.rating,
    })
    .collect();
  users.sort_by_key(|user| std::cmp::Reverse(user.rating));
  Ok(Json(users))
}

/// Interests recommended to those who like one interest.
#[utoipa::path(
  get, path = "/interests/{interest}/recommendations", tag = "profile",
  params(("interest" = String, Path, description = "the interest")),
  responses(
    (status = 200, body = Recommendations), (status = 400, body = api_types::ErrorBody),
    (status = 401, body = api_types::ErrorBody), (status = 502, body = api_types::ErrorBody),
    (status = 503, body = api_types::ErrorBody)
  )
)]
async fn related(
  State(app): State<AppState>,
  _caller: Authorized<AnyRole>,
  Path(interest): Path<String>,
) -> Result<Json<Recommendations>, ApiError> {
  let interest = interest_of(&interest)?;
  let reply = app
    .network
    .client()
    .get_interest_recommendations(&interest)
    .await
    .map_err(|e| unanswered("recommendations for the interest", e))?;
  // one list for both kinds, which `shown` splits by score
  Ok(Json(shown(soulseek_proto::server::Recommendations {
    recommendations: reply.recommendations,
    unrecommendations: Vec::new(),
  })))
}

/// Users who like one interest.
#[utoipa::path(
  get, path = "/interests/{interest}/users", tag = "profile",
  params(("interest" = String, Path, description = "the interest")),
  responses(
    (status = 200, body = Vec<String>), (status = 400, body = api_types::ErrorBody),
    (status = 401, body = api_types::ErrorBody), (status = 502, body = api_types::ErrorBody),
    (status = 503, body = api_types::ErrorBody)
  )
)]
async fn liked_by(
  State(app): State<AppState>,
  _caller: Authorized<AnyRole>,
  Path(interest): Path<String>,
) -> Result<Json<Vec<String>>, ApiError> {
  let interest = interest_of(&interest)?;
  let reply = app
    .network
    .client()
    .get_interest_similar_users(&interest)
    .await
    .map_err(|e| unanswered("users who like the interest", e))?;
  Ok(Json(reply.usernames))
}

#[cfg(test)]
mod tests {
  use soulseek_proto::server::{Recommendation, Recommendations};

  use super::*;

  fn scored(interest: &str, count: i32) -> Recommendation {
    Recommendation {
      interest: interest.into(),
      count,
    }
  }

  #[test]
  fn recommendations_are_merged_and_split_by_score() {
    let reply = Recommendations {
      recommendations: vec![scored("jazz", 2), scored("noise", -1), scored("folk", 5)],
      unrecommendations: vec![scored("polka", -4), scored("blues", 0)],
    };
    let sorted = sorted(reply);
    assert_eq!(
      sorted.recommendations,
      [scored("folk", 5), scored("jazz", 2)]
    );
    assert_eq!(
      sorted.unrecommendations,
      [scored("polka", -4), scored("noise", -1), scored("blues", 0)]
    );
  }

  #[tokio::test]
  async fn an_interests_recommendations_come_as_one_list_split_by_score() {
    use soulseek_proto::server::GetInterestRecommendationsResponse;
    use soulseek_proto::{ServerRequest, ServerResponse};

    use crate::testing::soulseek::{self, Server};
    use crate::testing::{TRUSTED, request};

    let app = soulseek::app(TRUSTED).await;
    let server = Server::start(|asked| match asked {
      ServerRequest::GetInterestRecommendations(asked) => {
        vec![ServerResponse::GetInterestRecommendations(
          GetInterestRecommendationsResponse {
            interest: asked.interest.clone(),
            recommendations: vec![scored("bebop", 2), scored("metal", -1), scored("swing", 5)],
          },
        )]
      }
      _ => Vec::new(),
    })
    .await;
    server.login(&app).await;
    let answer = app
      .send(request(
        "GET",
        "/api/v0/interests/jazz/recommendations",
        None,
      ))
      .await;
    assert_eq!(answer.status, 200, "{:?}", answer.body);
    assert_eq!(
      answer.json(),
      serde_json::json!({
        "recommended": [
          { "interest": "swing", "score": 5 },
          { "interest": "bebop", "score": 2 }
        ],
        "unrecommended": [{ "interest": "metal", "score": -1 }]
      })
    );
  }
}
