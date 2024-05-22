/* Fires an API call to the server and adds the reported city as an alien city
  */
function postAlienEncounter() {
    var city = $("#city-post-input").val();
 
    // Fires an Ajax call to the URL defined in the index.js function file
// All URLs to the Advanced I/O function will be of the pattern: /server/{function_name}/{url_path}
    $.ajax({
        url: "/server/alien_city_function/alien",
        type: "post",
        contentType: "application/json",
        data: JSON.stringify({
            "city_name": city
        }),
        success: function (data) {
            alert(data.message);
        },
        error: function (error) {
            alert(error.message);
        }
    });
}
 
/**
 * Fires an API call to the server to check whether the given city is alien city or not
 */
function getAlienEncounter() {
    showLoader();
    var positive = "https://media.giphy.com/media/Y1GYiLui9NHcxVKhdo/giphy.gif";
    var negative = "https://media.giphy.com/media/fsPcMdeXPxSP6zKxCA/giphy.gif";
    var city = $("#city-get-input").val();
 
  // Fires an Ajax call to the URL defined in the index.js function file
 // All URLs to the Advanced I/O function will be of the pattern: /server/{function_name}/{url_path}
    $.ajax({
        url: "/server/alien_city_function/alien?city_name=" + city,
        type: "get",
        success: function (data) {
            console.log(data);
            $("#result-text").text("");
            $("#result-text").text(data.message);
            var imagesrc = negative;
            if (data.signal == 'positive') {
                imagesrc = positive;
            }
            $("#result-image").html("");
            $("#result-image").html("");
            hideLoader();
        },
        errror: function (error) {
            alert(error.message);
        }
    });
}
 
function showLoader()
{
    $("#result-container").hide();
    $("#loader").show();
}
 
function hideLoader()
{
    $("#loader").hide();
    $("#result-container").show();
}