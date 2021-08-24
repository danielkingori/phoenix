# Getting Facebook Comments
This process helps a researcher identify and save comment threads on Facebook. It uses the following tools:
- Firefox web browser
- SingleFile plugin for Firefox to save the pages to a local directory

## Facebook comment download instructions
#### 1. Install Firefox
Install Firefox (if it’s not installed already).

#### 2. Install Single File
Open Firefox and install SingleFile on Firefox from this link.
When you install SingleFile, the plugin icon will show up in the upper right hand corner of the web browser.


#### 3. Set your Singlefile settings: 
Right click on the SingleFile icon and choose the option Manage extension in the middle of the menu.
In the SingleFile page, click Options just under the name of the extension.

**Filename**
- Add the following for the template file name: `fb_comments/{date-locale}_{time-locale}_{page-title}.html`
- Set max_length (bytes) to: `50`

**Autosave**
- set auto-save after page load or on page unload to: `ON`
- set auto-save waiting delay after page load (s) to: `0` _(this will make the autosave happen immediately)_

#### 4. Activate Single File in the browser.
Right click on the SingleFile icon and navigate to:
`SingleFile > Auto-save > Auto-save all tabs` -- make sure that option has the checkmark!

Watch the SingleFile icon for an `[A]` for auto-download. If it does not have the `[A]`, it will not auto download.
When SingleFile downloads a page, a little blue arrow appears with a small animation. This should appear whenever you navigate to a new page. This is how you know it’s working.

#### 5. Browse Facebook pages of interest
Navigate to `mbasic.facebook.com/(your target link)` and SingleFile will start saving everything you look at. 
It will only save content you see. 
There are up to 10 comments per page using mbasic.facebook.com

For each post, make sure to click `View more comments` or View previous comments, whichever way the thread is going. 
Some threads go forward and some go backward, so this requires some attention to detail.

Make sure to click every `View replies` link for any comment that has replies, so you can also save the subcomments.

In each subcomment thread, make sure to click `View previous replies`, because subcomment threads always go backward.

A good practice is to open each comment thread in a new tab and close it when you are done saving all the pages in the thread. 
This practice will help you keep your place in the main thread and keep your browser uncluttered.

On Windows, the profile will save files to your `Downloads/fb-comments/` folder if you set the filename location,
or simply `Downloads` if you didn't.

#### 6. Upload files.
Upload all the files you save to the proper folder for processing, in the cloud or locally.

#### 7. Parse the comments.
Next, run the facebook comment parsing script as per the [scrape overview](docs/scrape_overview.md).
