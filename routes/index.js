const express = require("express");
const router = express.Router();

const myDb = require("../db/mySqliteDB.js");

/* GET home page. */
router.get("/", async function (req, res) {
  res.redirect("/games");
});

// http://localhost:3000/references?pageSize=24&page=3&q=John
router.get("/games", async (req, res, next) => {
  const query = req.query.q || "";
  const page = +req.query.page || 1;
  const pageSize = +req.query.pageSize || 24;
  const msg = req.query.msg || null;
  try {
    let total = await myDb.getGamesCount(query);
    console.log("/games","total", total);
    let games = await myDb.getGames(query, page, pageSize);
    let athletes = null;
    let sports = null;
    let events = null;
    res.render("./pages/index", {
      games,
      athletes,
      sports,
      events,
      query,
      msg,
      currentPage: page,
      lastPage: Math.ceil(total/pageSize),
    });
  } catch (err) {
    next(err);
  }
});


router.get("/athletes", async (req, res, next) => {
  const query = req.query.q || "";
  const page = +req.query.page || 1;
  const pageSize = +req.query.pageSize || 24;
  const msg = req.query.msg || null;
  // console.log("===> athletes, req");
  try {
    let total = await myDb.getAthletesCount(query);
    let athletes = await myDb.getAthletes(query, page, pageSize);
    let games = null;
    let sports = null;
    let events = null;
    res.render("./pages/index", {
      athletes,
      games,
      query,
      events,
      sports,
      msg,
      currentPage: page,
      lastPage: Math.ceil(total/pageSize),
    });
  } catch (err) {
    next(err);
  }
});


router.get("/sports", async (req, res, next) => {
  const query = req.query.q || "";
  const page = +req.query.page || 1;
  const pageSize = +req.query.pageSize || 24;
  const msg = req.query.msg || null;
  // console.log("===> athletes, req");
  try {
    let total = await myDb.getSportsCount(query);
    let sports = await myDb.getSports(query, page, pageSize);
    let games = null;
    let athletes = null;
    let events = null;
    res.render("./pages/index", {
      sports,
      games,
      athletes,
      events,
      query,
      msg,
      currentPage: page,
      lastPage: Math.ceil(total/pageSize),
    });
  } catch (err) {
    next(err);
  }
});


router.get("/events", async (req, res, next) => {
  const query = req.query.q || "";
  const page = +req.query.page || 1;
  const pageSize = +req.query.pageSize || 24;
  const msg = req.query.msg || null;
  try {
    let total = await myDb.getEventsCount(query);
    let events = await myDb.getEvents(query, page, pageSize);
    let games = null;
    let athletes = null;
    let sports = null;
    res.render("./pages/index", {
      sports,
      events,
      games,
      athletes,
      query,
      msg,
      currentPage: page,
      lastPage: Math.ceil(total/pageSize),
    });
  } catch (err) {
    next(err);
  }
});

router.get("/athletes/:athleteID/edit", async (req, res, next) => {
  const athleteID = req.params.athleteID;

  const msg = req.query.msg || null;
  try {

    let athlete = await myDb.getAthleteByID(athleteID);

    console.log("edit atheletes", {
      athlete,
      msg,
    });


    res.render("./pages/editAthlete", {
      athlete,
      msg,
    });
  } catch (err) {
    next(err);
  }
});

router.post("/athletes/:athleteID/edit", async (req, res, next) => {
  const athleteID = req.params.athleteID;
  const ref = req.body;

  try {

    let updateResult = await myDb.updateAthletesByID(athleteID, ref);
    console.log("update", updateResult);

    if (updateResult && updateResult.changes === 1) {
      res.redirect("/athletes/?msg=Updated");
    } else {
      res.redirect("/athletes/?msg=Error Updating");
    }

  } catch (err) {
    next(err);
  }
});

router.post("/references/:reference_id/addAuthor", async (req, res, next) => {
  console.log("Add author", req.body);
  const reference_id = req.params.reference_id;
  const author_id = req.body.author_id;

  try {

    let updateResult = await myDb.addAuthorIDToReferenceID(reference_id, author_id);
    console.log("addAuthorIDToReferenceID", updateResult);

    if (updateResult && updateResult.changes === 1) {
      res.redirect(`/references/${reference_id}/edit?msg=Author added`);
    } else {
      res.redirect(`/references/${reference_id}/edit?msg=Error adding author`);
    }

  } catch (err) {
    next(err);
  }
});

router.get("/athletes/:athleteID/delete", async (req, res, next) => {
  const athleteID = req.params.athleteID;

  try {

    let deleteResult = await myDb.deleteAthletesByID(athleteID);
    console.log("delete", deleteResult);

    if (deleteResult && deleteResult.changes === 1) {
      res.redirect("/athletes/?msg=Deleted");
    } else {
      res.redirect("/athletes/?msg=Error Deleting");
    }

  } catch (err) {
    next(err);
  }
});

router.post("/createAthlete", async (req, res, next) => {
  const ref = req.body;
  console.log("medal", ref.medal);
  try {
    await myDb.createAthlete(ref);

    //console.log("Inserted", insertRes);
    res.redirect("/athletes/?msg=New athlete created.");
  } catch (err) {
    res.redirect("/athletes/?msg=Error creating athlete.");
    next(err);
  }
});

module.exports = router;
