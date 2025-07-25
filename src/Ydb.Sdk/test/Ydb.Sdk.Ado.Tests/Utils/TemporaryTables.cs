namespace Ydb.Sdk.Ado.Tests.Utils;

public class TemporaryTables<T>
{
    public readonly string Series = $"`series_from_{typeof(T).Name}`";
    public readonly string Seasons = $"`seasons_from_{typeof(T).Name}`";
    public readonly string Episodes = $"`episodes_from_{typeof(T).Name}`";

    public string CreateTables => $"""
                                   CREATE TABLE {Series}
                                   (
                                       series_id Uint64,
                                       title Text,
                                       series_info Text,
                                       release_date Date,
                                       PRIMARY KEY (series_id)
                                   );
                                   CREATE TABLE {Seasons}
                                   (
                                       series_id Uint64,
                                       season_id Uint64,
                                       title Text,
                                       first_aired Date,
                                       last_aired Date,
                                       PRIMARY KEY (series_id, season_id)
                                   );
                                   CREATE TABLE {Episodes}
                                   (
                                       series_id Uint64,
                                       season_id Uint64,
                                       episode_id Uint64,
                                       title Text,
                                       air_date Date,
                                       PRIMARY KEY (series_id, season_id, episode_id)
                                   );
                                   """;

    public string UpsertData =>
        $"""
         UPSERT INTO {Series} (series_id, title, release_date, series_info)
         VALUES (1, "IT Crowd", Date("2006-02-03"),
         "The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry."),
         (2, "Silicon Valley", Date("2014-04-06"),
         "Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.");
         UPSERT INTO {Seasons} (series_id, season_id, title, first_aired, last_aired)
         VALUES (1, 1, "Season 1", Date("2006-02-03"), Date("2006-03-03")),
             (1, 2, "Season 2", Date("2007-08-24"), Date("2007-09-28")),
             (1, 3, "Season 3", Date("2008-11-21"), Date("2008-12-26")),
             (1, 4, "Season 4", Date("2010-06-25"), Date("2010-07-30")),
             (2, 1, "Season 1", Date("2014-04-06"), Date("2014-06-01")),
             (2, 2, "Season 2", Date("2015-04-12"), Date("2015-06-14")),
             (2, 3, "Season 3", Date("2016-04-24"), Date("2016-06-26")),
             (2, 4, "Season 4", Date("2017-04-23"), Date("2017-06-25")),
             (2, 5, "Season 5", Date("2018-03-25"), Date("2018-05-13"));
         UPSERT INTO {Episodes} (series_id, season_id, episode_id, title, air_date)
         VALUES (1, 1, 1, "Yesterday's Jam", Date("2006-02-03")),
             (1, 1, 2, "Calamity Jen", Date("2006-02-03")),
             (1, 1, 3, "Fifty-Fifty", Date("2006-02-10")),
             (1, 1, 4, "The Red Door", Date("2006-02-17")),
             (1, 1, 5, "The Haunting of Bill Crouse", Date("2006-02-24")),
             (1, 1, 6, "Aunt Irma Visits", Date("2006-03-03")),
             (1, 2, 1, "The Work Outing", Date("2006-08-24")),
             (1, 2, 2, "Return of the Golden Child", Date("2007-08-31")),
             (1, 2, 3, "Moss and the German", Date("2007-09-07")),
             (1, 2, 4, "The Dinner Party", Date("2007-09-14")),
             (1, 2, 5, "Smoke and Mirrors", Date("2007-09-21")),
             (1, 2, 6, "Men Without Women", Date("2007-09-28")),
             (1, 3, 1, "From Hell", Date("2008-11-21")),
             (1, 3, 2, "Are We Not Men?", Date("2008-11-28")),
             (1, 3, 3, "Tramps Like Us", Date("2008-12-05")),
             (1, 3, 4, "The Speech", Date("2008-12-12")),
             (1, 3, 5, "Friendface", Date("2008-12-19")),
             (1, 3, 6, "Calendar Geeks", Date("2008-12-26")),
             (1, 4, 1, "Jen The Fredo", Date("2010-06-25")),
             (1, 4, 2, "The Final Countdown", Date("2010-07-02")),
             (1, 4, 3, "Something Happened", Date("2010-07-09")),
             (1, 4, 4, "Italian For Beginners", Date("2010-07-16")),
             (1, 4, 5, "Bad Boys", Date("2010-07-23")),
             (1, 4, 6, "Reynholm vs Reynholm", Date("2010-07-30")),
             (2, 1, 1, "Minimum Viable Product", Date("2014-04-06")),
             (2, 1, 2, "The Cap Table", Date("2014-04-13")),
             (2, 1, 3, "Articles of Incorporation", Date("2014-04-20")),
             (2, 1, 4, "Fiduciary Duties", Date("2014-04-27")),
             (2, 1, 5, "Signaling Risk", Date("2014-05-04")),
             (2, 1, 6, "Third Party Insourcing", Date("2014-05-11")),
             (2, 1, 7, "Proof of Concept", Date("2014-05-18")),
             (2, 1, 8, "Optimal Tip-to-Tip Efficiency", Date("2014-06-01")),
             (2, 2, 1, "Sand Hill Shuffle", Date("2015-04-12")),
             (2, 2, 2, "Runaway Devaluation", Date("2015-04-19")),
             (2, 2, 3, "Bad Money", Date("2015-04-26")),
             (2, 2, 4, "The Lady", Date("2015-05-03")),
             (2, 2, 5, "Server Space", Date("2015-05-10")),
             (2, 2, 6, "Homicide", Date("2015-05-17")),
             (2, 2, 7, "Adult Content", Date("2015-05-24")),
             (2, 2, 8, "White Hat/Black Hat", Date("2015-05-31")),
             (2, 2, 9, "Binding Arbitration", Date("2015-06-07")),
             (2, 2, 10, "Two Days of the Condor", Date("2015-06-14")),
             (2, 3, 1, "Founder Friendly", Date("2016-04-24")),
             (2, 3, 2, "Two in the Box", Date("2016-05-01")),
             (2, 3, 3, "Meinertzhagen's Haversack", Date("2016-05-08")),
             (2, 3, 4, "Maleant Data Systems Solutions", Date("2016-05-15")),
             (2, 3, 5, "The DefaultInstance Chair", Date("2016-05-22")),
             (2, 3, 6, "Bachmanity Insanity", Date("2016-05-29")),
             (2, 3, 7, "To Build a Better Beta", Date("2016-06-05")),
             (2, 3, 8, "Bachman's Earnings Over-Ride", Date("2016-06-12")),
             (2, 3, 9, "Daily Active Users", Date("2016-06-19")),
             (2, 3, 10, "The Uptick", Date("2016-06-26")),
             (2, 4, 1, "Success Failure", Date("2017-04-23")),
             (2, 4, 2, "Terms of Service", Date("2017-04-30")),
             (2, 4, 3, "Intellectual Property", Date("2017-05-07")),
             (2, 4, 4, "Teambuilding Exercise", Date("2017-05-14")),
             (2, 4, 5, "The Blood Boy", Date("2017-05-21")),
             (2, 4, 6, "Customer Service", Date("2017-05-28")),
             (2, 4, 7, "The Patent Troll", Date("2017-06-04")),
             (2, 4, 8, "The Keenan Vortex", Date("2017-06-11")),
             (2, 4, 9, "Hooli-Con", Date("2017-06-18")),
             (2, 4, 10, "Server Error", Date("2017-06-25")),
             (2, 5, 1, "Grow Fast or Die Slow", Date("2018-03-25")),
             (2, 5, 2, "Reorientation", Date("2018-04-01")),
             (2, 5, 3, "Chief Operating Officer", Date("2018-04-08")),
             (2, 5, 4, "Tech Evangelist", Date("2018-04-15")),
             (2, 5, 5, "Facial Recognition", Date("2018-04-22")),
             (2, 5, 6, "Artificial Emotional Intelligence", Date("2018-04-29")),
             (2, 5, 7, "Initial Coin Offering", Date("2018-05-06")),
             (2, 5, 8, "Fifty-One Percent", Date("2018-05-13"));
         """;

    public string DeleteTables => $"DROP TABLE {Series}; DROP TABLE {Seasons}; DROP TABLE {Episodes};";
}
