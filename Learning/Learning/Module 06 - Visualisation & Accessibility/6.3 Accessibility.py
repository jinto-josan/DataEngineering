# Databricks notebook source
# MAGIC %md
# MAGIC # Overview
# MAGIC Visualizing data using tables, graphs, videos, or images can help in making the data itself or stories and takeaways
# MAGIC related to it more accessible.
# MAGIC However, these tools can also have the opposite effect for some users, making the output we provide less accessible.
# MAGIC This is why it is important to know our audience. Who is going to access our graphs? Where are they integrated? Will
# MAGIC the content be shared with others?
# MAGIC
# MAGIC ## Diverse abilities, disabilities, and barriers
# MAGIC [People have different abilities, skills, and preferences](https://www.w3.org/WAI/people-use-web/abilities-barriers/).
# MAGIC So it's safe to say that they behave and interact with interfaces—and data presented—differently.
# MAGIC
# MAGIC Examples for different kinds of disabilities (non-exhaustive list):
# MAGIC - Visual (e.g. color blindness)
# MAGIC - Cognitive, learning, and neurological (e.g. dyslexia)
# MAGIC - Physical (e.g. parkinson's disease)
# MAGIC - Auditory (partial or total inability to hear)
# MAGIC - Speech (e.g. stuttering)
# MAGIC
# MAGIC Features that provide accessibility to some might be inaccessible to others and it is also possible that a person has a combination of disabilities, e.g. deaf-blindness (including partial loss of sight and hearing).
# MAGIC The two-senses or multi-sensory principle can help increase accessibility: make information perceivable by at least two senses.
# MAGIC
# MAGIC ## Required by some, helpful for others
# MAGIC Accessibility features can be helpful for everyone. Even some everyday objects were actually invented as tools for accessibility, like the electrical toothbrush, or vibration alarm for mobile phones. Similarly, accessibility features for digital content can increase the usability for all users, for example by providing closed captions for videos, or providing audio for long texts.
# MAGIC
# MAGIC 🕒 **Estimated Time For Completion:** 20 minutes
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Graphical visualization
# MAGIC
# MAGIC As stated above graphs can be one form to visualize data. Especially for those there are some criteria which can be 
# MAGIC considered regarding the design or the technical integration of the graphs on a web-based interface.
# MAGIC
# MAGIC ## Design choices
# MAGIC
# MAGIC ### Keeping it simple
# MAGIC
# MAGIC _“Data graphics should draw the viewer’s attention to the sense and substance of the data, not to something else. […]”_ Edward Tufte - [The Visual Display of Quantitative Information](https://www.edwardtufte.com/tufte/books_vdqi). Keep in mind to:
# MAGIC
# MAGIC - Reduce clutter.
# MAGIC - Avoid animations (and if they are necessary, provide a way to turn them off).
# MAGIC - If possible: offer a light and a dark version (e.g., for people who are sensitive to bright colors).
# MAGIC - Using scalable image formats such as SVG helps users to increase and decrease the size of the image without loss of detail/information.
# MAGIC
# MAGIC ![A comparison of two graphs. The left graph is a simple 2D bar chart with only the most basic elements. The right graph is presented in 3D with shadows. It is hard to see which value the bars reach in the right graph](./assets/image2.png)
# MAGIC
# MAGIC ### Colors
# MAGIC
# MAGIC Due to the fact that [around 300 million people have color-deficient vision](https://www.colourblindawareness.org/), 
# MAGIC considering the choice of color can be crucial for the accessibility of visualizations.
# MAGIC
# MAGIC ### Contrast
# MAGIC
# MAGIC * Use contrasts / different shades instead of multiple colors
# MAGIC     * A suggested level of contrast ratio for objects is **3:1**
# MAGIC     * Tools exist that can be used for checking the contrast of two colors manually:
# MAGIC         * [Contrast Checker by WebAIM](https://webaim.org/resources/contrastchecker/)
# MAGIC         * [Contrast Ratio by Siege Media](https://www.siegemedia.com/contrast-ratio)
# MAGIC
# MAGIC Some challenges might still remain:
# MAGIC
# MAGIC * Using a color palette might comply with required contrast ratios but on the other hand might distract the focus in charts from the most important information for the user (e.g. when different colors are used for error-events in a stacked bar-chart)
# MAGIC
# MAGIC ### Conveying meaning
# MAGIC
# MAGIC * If colors convey meaning, provide additional ways of differentiating
# MAGIC * Tools for that can include symbols, labels, patterns, texture, icon, text or overlay
# MAGIC * The disadvantage of additional patterns or elements in the visualization can be that it will make it even more difficult to read the information in a visualization, therefore one should avoid adding too much [chartjunk](https://en.wikipedia.org/wiki/Chartjunk).
# MAGIC
# MAGIC ![A simple example of a pie chart where patterns are used in addition to colors to keep the areas visually apart and label them.](./assets/image1.png)  
# MAGIC
# MAGIC **DISCLAIMER**: this example image is only for illustrating the usage of patterns in addition to colors. However, it is not a good example for a chart in general. There are better ways to label charts, and the patterns and colors can be distracting instead of helping.
# MAGIC
# MAGIC
# MAGIC ### Alternative texts and descriptions
# MAGIC
# MAGIC * Provide short, concise alternative texts.
# MAGIC * There is no universally applicable alternative text for an image. The context matters.
# MAGIC * If applicable, provide a longer description, e.g. below the image.
# MAGIC * Don't start the text with "an image" or "a picture". Screen reader users already know this, because you're using the HTML image element.
# MAGIC
# MAGIC ### More resources & examples
# MAGIC
# MAGIC * [Harvard University: Write helpful Alt Text to describe images](https://accessibility.huit.harvard.edu/describe-content-images)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Text accessibility 
# MAGIC
# MAGIC When working with or presenting data or data related information, it's a good idea to **adhere to proven accessibility principles**. These standards are inspired by web design accessibility practices as well as standards from print to ensure that all users, including those with disabilities, can access and comprehend the content we present.  
# MAGIC
# MAGIC **Text accessibility** focuses on making written content easy to read and understand for everyone, regardless of their physical or cognitive abilities. This includes considering visual impairments, cognitive challenges, and language barriers.   
# MAGIC
# MAGIC Let's have a look at different elements of **text accessibility**, provide practical guidelines for improving the readability and usability of our content, and explain why each point is important.
# MAGIC
# MAGIC ## Text Size and Adjustability
# MAGIC
# MAGIC ### Size Matters
# MAGIC
# MAGIC The size of the presented text is fundamental to accessibility. Small text can be difficult to read, especially for users with visual impairments. **Larger text is easier to read**, reducing eye strain and making it accessible to a wider audience.   
# MAGIC
# MAGIC It can be helpful to give context to the data, **without context you may lose your audience**. Delight your readers with easy-to-read texts and don't tire them out.   
# MAGIC
# MAGIC This applies both to descriptive content and to legends, notes and information.
# MAGIC
# MAGIC ### Adjustable Text Size
# MAGIC Allowing users to change the text size to suit their preferences is essential. This supports users with varying degrees of visual impairment and those who prefer larger text for comfort. Text resizing should not break the layout of your presentation. Remember that **the text should enrich your data and diagrams**, so they should be in focus. Consider resizing these elements along with the text.
# MAGIC
# MAGIC - **Good Example**: Using relative units like `font-size: 1em;` so users can adjust text size via browser settings.
# MAGIC - **Bad Example**: Using fixed units like `font-size: 12px;` which doesn't scale with user preferences.
# MAGIC
# MAGIC ## Choosing the Right Font
# MAGIC
# MAGIC ### Font Selection
# MAGIC The type of font used can significantly impact readability. `Sans-Serif` fonts, such as `Arial`, `Helvetica`, and `Verdana`, are generally easier to read on screens compared to serif fonts. Avoiding overly decorative fonts ensures the text is straightforward and readable.
# MAGIC
# MAGIC - **Good Example**: Using a `Sans-Serif` font for body text.
# MAGIC - **Bad Example**: Using a decorative font like `Comic Sans` or a complex script font for body text.
# MAGIC
# MAGIC ### Formatting for Clarity
# MAGIC **Text formatting** should be clean and straightforward. Adequate line spacing and high contrast between text and background enhance readability, particularly for users with visual impairments or dyslexia. But even for those who read for longer, it becomes less tiring to read texts with high contrast.
# MAGIC
# MAGIC - **Good Example**: Text with line spacing set to 1.4 and high contrast colors (black text on a white background).
# MAGIC - **Bad Example**: Text with minimal line spacing and low contrast colors (light gray text on a white background).
# MAGIC
# MAGIC ## Consistency and Clear Markup
# MAGIC
# MAGIC ### Consistent Markup
# MAGIC Using consistent markup for headings, paragraphs, lists, and other elements helps screen readers understand the structure of the content. This aids navigation and comprehension for users relying on assistive technologies. Keeping this in mind, it also helps you to write information in a structured order that leads to better understandable content.
# MAGIC
# MAGIC - **Good Example**: Using `<h1>` for the main title, `<h2>` for section headings, and `<p>` for paragraphs.
# MAGIC - **Bad Example**: Using `<div>` tags with classes to style text, without semantic meaning.
# MAGIC
# MAGIC ### Concise and Understandable Formatting
# MAGIC Keeping the text concise and to the point makes information easier to digest. Using bullet points and numbered lists breaks up large blocks of text, which is beneficial for all users, especially those with cognitive impairments.
# MAGIC
# MAGIC - **Good Example**: Using bullet points for lists and keeping paragraphs short.
# MAGIC - **Bad Example**: Long, unbroken paragraphs filled with complex sentences.
# MAGIC
# MAGIC Distinguish between ordered and unordered lists to provide structural information about the content.
# MAGIC
# MAGIC ## Language and Target Audience
# MAGIC
# MAGIC ### Language Settings
# MAGIC Setting the language of the presentation (eg. a web page) correctly using the lang attribute in HTML helps screen readers pronounce words correctly and provides context for translations. This also applies to PDFs and other formats. This improves the experience for non-native speakers and those using translation tools.
# MAGIC
# MAGIC - **Good Example**: `<html lang="en">`
# MAGIC - **Bad Example**: `<html>`, without specifying the language.
# MAGIC
# MAGIC Pay attention to the metadata of the elements and of the overall document. They are just as important as the content itself. Especially when a reader uses technology to support the eyes.
# MAGIC
# MAGIC ### Supported Translations
# MAGIC Knowing your audience and where they are coming from helps to determine if the content should be translated and which languages should be supported. Accurate translations ensure that non-native speakers can understand and engage with your content just as well as native speakers.
# MAGIC
# MAGIC In times when we are close to the babelfish, this requirement increases. Make sure that your text can be translated automatically.
# MAGIC
# MAGIC - **Good Example**: Providing content in multiple languages written by native speakers (if possible).
# MAGIC - **Bad Example**: Offering no translation or only poor machine translations that are difficult to understand.
# MAGIC
# MAGIC Providing texts in e.g. fluent English and fluent Spanish improves the possibility of translating the same text into e.g. Japanese, as the meaning of the words is more accurate than if the text is written only by a native English speaker.
# MAGIC
# MAGIC ### Depth of Explanations
# MAGIC Providing explanations that match your target audience's knowledge level is crucial. Offering expandable sections or links to more detailed information caters to both novice and experienced users.
# MAGIC
# MAGIC - **Good Example**: Using simple explanations with links to more detailed content.
# MAGIC - **Bad Example**: Using complex explanations without additional context or simpler alternatives or use one large text block that repeats itself over and over in boring language.
# MAGIC
# MAGIC ### Easy Language
# MAGIC Using plain language and short sentences ensures the content is accessible to a wider audience, including those with cognitive impairments or lower literacy levels. Complex words and sentence structures can alienate or confuse users. 
# MAGIC
# MAGIC This does not imply that the text must get boring. Sometimes nested sentences don't give more information, but hide essential information behind clever-sounding phrases.
# MAGIC
# MAGIC In certain cases, not using a domain specific language  and instead trying to specifically avoid the common words or names can lead to misunderstandings, especially when talking to a more advanced audience.
# MAGIC
# MAGIC - **Good Example**: "The personal information of 20 Million people was stolen in the breach."
# MAGIC - **Bad Example**: "Through a series of using a chain of specific 0-days the intruders were able to obtain PII from CRM systems leading to the exposure of a data set of more than 20 Million unique entries."
# MAGIC
# MAGIC ### Avoiding Jargon and Pop Culture References
# MAGIC Avoiding industry jargon, technical terms, and pop culture references ensures the content is understandable to everyone, not just experts or culturally specific groups. When it comes to very specific terms or jargon that cannot be avoided, clear definitions should be provided or further explanations added. When presenting to audiences, we have a tendency to want to look smart and include more technical terms, even though this might lead to parts of the audience feeling left out.
# MAGIC
# MAGIC - **Good Example**: "We formatted the data in a way to show rainfall in given months in the following graph."
# MAGIC - **Bad Example**: "We dissected the Pandas dataframe using a splice operation to display the rain/month ratio on this Plotly polyline mark."
# MAGIC
# MAGIC ### Explaining Abbreviations
# MAGIC Abbreviations should be spelled out in full the first time they are used to help all users understand them, including those who may not be familiar with the terms. This practice aids comprehension and inclusivity.
# MAGIC
# MAGIC - **Good Example**: "HyperText Markup Language (HTML) is used to create web pages."
# MAGIC - **Bad Example**: "HTML is used to create web pages." (without explaining HTML)
# MAGIC
# MAGIC ## Reading Level
# MAGIC
# MAGIC ### Appropriate Reading Level
# MAGIC Writing at a reading level that matches your target audience ensures that the content is accessible to most users. Tools like the Flesch-Kincaid or LIX readability tests can help ensure your text is suitable. Generally, content should be accessible to users with a 6th to 8th-grade reading level. In some instances, it might make sense to provide different variations of a text for different target audiences but usually it is preferred to provide a single version of the content that can be understood by everyone.
# MAGIC
# MAGIC - **Good Example**: This sentence shows the point when you read it.
# MAGIC - **Bad Example**: This sentence, taken as a reading passage unto itself, is being used to prove a point.
# MAGIC
# MAGIC ## Text-to-Speech and Navigation
# MAGIC
# MAGIC ### Text-to-Speech Compatibility
# MAGIC Ensuring your website is compatible with **text-to-speech (TTS)** software makes your content accessible to users with visual impairments or reading difficulties. Proper HTML markup 
# MAGIC using [**semantic HTML**](https://www.w3schools.com/html/html5_semantic_elements.asp) ensures TTS programs can accurately interpret and vocalize the content.
# MAGIC
# MAGIC - **Good Example**: Using semantic HTML elements and ARIA labels to enhance TTS functionality.
# MAGIC - **Bad Example**: Using non-semantic tags (like only divs) and lacking ARIA labels, confusing TTS programs.
# MAGIC
# MAGIC > Did you know: iOS has an embedded text-to-speech reader you can turn on with a two-finger swipe-down gesture. On Android there is also an accessibility function when you hold both volume buttons down for 3 seconds.
# MAGIC
# MAGIC ### Reader Support and Navigation Jumps
# MAGIC On websites, including features like [skip links](https://www.w3schools.com/accessibility/accessibility_skip_links.php) allows users to jump directly to the main content, supporting easier navigation for screen reader users and those using keyboard navigation. Use a descriptive navigation text that precisely describes the content. This improves the overall user experience.
# MAGIC
# MAGIC - **Good Example**: Providing a "Skip to main content" link at the top of the page.
# MAGIC - **Bad Example**: Forcing users to tab through an entire navigation menu before reaching the main content.
# MAGIC - **Bad Example**: Having the tab order set incorrectly which makes users using keyboard navigation jump all over the place or start at the bottom of the conent.
# MAGIC
# MAGIC ## Additional Features
# MAGIC
# MAGIC ### Summaries and optional content
# MAGIC Summaries at the top and/or bottom of longer paragraphs or the whole content can help to double check if the information was understood correctly or provide alternative phrasing. Trade-offs have to be made to not repeat the same points too often or unnecessarily bloat the content.
# MAGIC
# MAGIC It has proven to be a good idea to start with a "Management Summary" paragraph stating what the text is all about and what topics it will cover, and end with a bullet point list summarizing the content.
# MAGIC
# MAGIC Content that is optional should be thought about: In some cases it pays of to not include it at all, in other circumstances it makes sense to keep it but mark it as optional.
# MAGIC
# MAGIC ### Print Preview
# MAGIC Providing a print-friendly version of your information ensures that all users can easily print and read the content offline without losing important information. Print stylesheets can format content appropriately.
# MAGIC
# MAGIC - **Good Example**: Offering a print stylesheet that formats only the relevant content appropriately for printing.
# MAGIC - **Bad Example**: Printing the whole page as it appears on the screen, which may include unnecessary navigation menus and advertisements.
# MAGIC
# MAGIC ### Following Disability Language Style Guide
# MAGIC The language we use should be respectful and inclusive. Using person-first language and avoiding outdated or offensive terms helps to create a more inclusive environment.
# MAGIC
# MAGIC - **Good Example**: "Person with a disability" instead of "disabled person."
# MAGIC - **Bad Example**: "Handicapped" or other outdated terms.
# MAGIC
# MAGIC [Link to a disability language style guide](https://ncdj.org/style-guide/)
# MAGIC
# MAGIC ### Providing Full Link Text
# MAGIC On websites, using descriptive link text instead of vague phrases helps users understand the purpose and destination of the link. This practice enhances navigation and accessibility, particularly for screen reader users.
# MAGIC
# MAGIC - **Good Example**: "Learn more about our accessibility features."
# MAGIC - **Bad Example**: "Click here."

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Tables
# MAGIC
# MAGIC ## Keeping it simple
# MAGIC
# MAGIC * Try to keep your tables as simple as possible.
# MAGIC * The most simple table should have one row or column header.
# MAGIC * If the data itself is complex, think about whether it’s possible to split it into multiple tables. This might also increase accessibility on mobile devices.
# MAGIC
# MAGIC ### Use semantic HTML
# MAGIC
# MAGIC * If you’re publishing your tables on the web, use semantic markup for creating your tables.
# MAGIC * **Good:** Using `<table>`, `<tr>`, `<th>`, `<td>,` and other table markup to present tabular data.
# MAGIC * **Bad:** Using non-semantic HTML like `<div>` instead, just styling it like a table.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC | Previous Topic                                                  | Next Topic                                                            |
# MAGIC |-----------------------------------------------------------------|-----------------------------------------------------------------------|
# MAGIC | <a href="$./6.2 Visualisation" target="_self">Visualisation</a> | <a href="$./6.4 Visualisation II" target="_self">Visualisation II</a> |
# MAGIC
# MAGIC &copy; 2024 Thoughtworks. All rights reserved.<br/>