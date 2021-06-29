// var links = document.querySelectorAll("a"), link_list = [];
//
// function getLinks(){
//   return document.querySelectorAll("a")
// }
//
//
// function getLinkList(links, link_list)
// for (var i=1; i < links.length; i++){
//   if(links[i].text.includes("View previous comments")
//      || links[i].text.includes(" replied ") || links[i].text.includes("View previous replies")){
//     link_list.push(links[i]);
//   }
// }
//
// link_list.length;


//
// const sleep = (milliseconds) => {
//   return new Promise(resolve => setTimeout(resolve, milliseconds))
// }



function getLinks(){
  return document.querySelectorAll("a");
}


function getFirstLink(links){
  for(var i=0; i < links.length; i++){
    text = links[i].text;
    if (text.includes("View previous comments") || text.includes("View more comments")){
      return text;
    }
  }
}


function addToLinkList(links, link_list, direction) {
  for(var i=0; i < links.length; i++) {
    if (links[i].text == direction || links[i].text.includes(" replied ")) {
      link_list.push(links[i])
    }
  }
  return link_list
}


function run() {

  var links = getLinks();
  var direction = getFirstLink(links);
  var link_list = [];
  link_list = addToLinkList(links, link_list, direction);

  var click_link = link_list.pop();
  // TODO this needs to be slowed down and I don't understand that yet in js
  // It's currently probably going to crash after the last link_list item
  while(link_list.length >= 0) {
    click_link.click();
    new_links = getLinks()
    link_list = addToLinkList(links, link_list, direction);
    click_link = link_list.pop();
    break;
  }
}

function openPage() {
  browser.tabs.create({
    url: "https://developer.mozilla.org"
  });
}

browser.browserAction.onClicked.addListener(openPage);
