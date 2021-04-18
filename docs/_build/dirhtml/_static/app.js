$(document).ready(function () {
  $(window).on(
    "resize",
    Foundation.utils.throttle(function (e) {
      if (Foundation.utils.is_small_only()) {
        $(".topbar_continer").removeClass("contain-to-grid");
      } else {
        $(".topbar_continer").addClass("contain-to-grid");
      }
    }, 300)
  );
});

// Opens external links in a new tab
$(document).ready(function () {
  const isExternal = new RegExp("^(?:[a-z]+:)?//", "i");
  $("a.reference").each(function () {
    $(this).removeClass("internal external");

    if (isExternal.test($(this).attr("href"))) {
      $(this).addClass("external");
      $(this).attr("target", "_blank");
    } else {
      $(this).addClass("internal");
    }
  });
});

// Displays a custom banner on top
if (!$.cookie("hide-enterprise-banner")) {
  $(".custom-promo-banner-wrap").show();
  $("body").css("padding-top", $(".custom-promo-banner-wrap").outerHeight());
} else {
  $(".custom-promo-banner-wrap").hide();
}

$(".custom-promo-banner__close").on("click", function () {
  $.cookie("hide-enterprise-banner", "1");
  $("body").css("padding-top", 0);
  $(".custom-promo-banner-wrap").hide();
});

$(window).resize(function () {
  if ($(".custom-promo-banner-wrap").is(":visible")) {
    $("body").css("padding-top", $(".custom-promo-banner-wrap").outerHeight());
  }
});
