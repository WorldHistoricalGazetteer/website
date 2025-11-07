/*
  Author: Stephen Gadd, Docuracy Ltd
  File: toggle-truncate.js
  Description: JQuery HTML truncation toggler
  
  This JQuery extension resolves the difficulties of truncating text which has embedded HTML tags.
  
  Sample usage:

  $('.some-class').toggleTruncate(); <== use default maxChars
  $('#some-element').toggleTruncate(42); <== override default maxChars
  $('.some-class').toggleTruncate(18, { colour: 'red', lessText: 'Enough Already' }); <== override maxChars and named parameters
  $('#some-element').toggleTruncate({ ellipsis: '', moreText: '(...)' }); <== use default maxChars and override named parameters

  Copyright (c) 2024 Stephen Gadd
  Licensed under the Creative Commons Attribution 4.0 International License (CC-BY 4.0).
*/

$.fn.toggleTruncate = function(arg1, arg2 = {}) {
	
	const maxChars = typeof arg1 === 'number' ? arg1 : 130; // <== Set the default as an integer at the end of this line
	const parameters = typeof arg1 === 'number' ? arg2 : arg1;
		
	// Customise defaults as required, and override with parameters when calling if necessary
	const defaultOptions = {
		ellipsis: '...', 
		moreText: 'More', 
		lessText: 'Less', 
		colour: '#000', 
		backgroundColour: '#daebf1',
		borderColour: '#ced4da',
		backgroundColourHover: '#aad2df',
		toggleTruncate: 'toggle-truncate' // basename for inserted classes: change needed only to resolve any conflict with existing code
	}
	
	const options = {...defaultOptions, ...parameters};
	
    // Inject required CSS styles into the head of the document if not done already
    if (!$(`style#${options.toggleTruncate}-styles`).length) {
        const toggleTruncateStyles = `
			.${options.toggleTruncate}-link {
			    display: inline-block;
			    cursor: pointer;
			    font-size: 0.7rem;
			    vertical-align: text-bottom;
			    line-height: normal;
			}
			.${options.toggleTruncate}-wrapper.${options.toggleTruncate}-more .${options.toggleTruncate} {
			    display: none;
			}
			.${options.toggleTruncate}-link > span {
			    display: inline-block;
			    padding: 0.2rem 0.4rem;
			    background-color: ${options.backgroundColour};
			    border: 1px solid ${options.borderColour};
			    color: ${options.colour};
			    border-radius: 0.2rem;
			    margin-left: 0.4rem;
			}
			.${options.toggleTruncate}-link > span:hover {
			    background-color: ${options.backgroundColourHover};
			}
			.${options.toggleTruncate}-wrapper:not(.${options.toggleTruncate}-more) .${options.toggleTruncate}-link > span:first-of-type,
			.${options.toggleTruncate}-wrapper:not(.${options.toggleTruncate}-more) .${options.toggleTruncate}-ellipsis {
			    display: none;
			}
        `;

        $('<style>')
            .prop('type', 'text/css')
            .prop('id', 'toggle-truncate-styles')
            .html(toggleTruncateStyles)
            .appendTo('head');
    }	
	
    return this.each(function() {
        const el = $(this);
        if (el.text().trim().length <= maxChars) return;
        const tagRegex = /(<\/?(?:a|b|br|div|em|h[1-6]|i|p|span|strong|u)[^>]*>)/;
        let charCount = 0;

        const wrappedContent = el.html().split(tagRegex).map(segment => {
            if (segment.startsWith('<')) {
                if (charCount > maxChars && !segment.startsWith('</')) {
					// Required for hiding tags which may have an otherwise-visible pseudo-element, such as fontawesome icons ('fas').
                    const classRegex = /class\s*=\s*["']([^"']+)["']/;
                    const classNames = (segment.match(classRegex) || [])[1];
                    if (classNames) {
                        const classes = classNames.split(/\s+/).filter(className => className.trim() !== '');
                        classes.push(`${options.toggleTruncate}`);
                        const updatedClassNames = classes.join(' ');
                        segment = segment.replace(classRegex, `class="${updatedClassNames}"`);
                    }
                    else {
                        segment = segment.replace('>', ` class="${options.toggleTruncate}">`);
                    }
                }
                return segment; // Preserve HTML tags
            } else {
                if (charCount > maxChars) {
                    return `<span class="${options.toggleTruncate}">${segment}</span>`;
                }
                else {
                    charCount += segment.length;
                    if (charCount <= maxChars) {
                        return segment
                    }
                    else {
                        const splitIndex = maxChars - (charCount - segment.length);
                        return `${segment.slice(0, splitIndex)}<span class="${options.toggleTruncate}">${segment.slice(splitIndex)}</span>`;
                    }
                }
            }
        }).join('');

        el.html(`<span class="${options.toggleTruncate}-wrapper ${options.toggleTruncate}-more">${wrappedContent}<span class="${options.toggleTruncate}-ellipsis">${options.ellipsis}</span><span class="${options.toggleTruncate}-link"><span>${options.moreText}</span><span class="${options.toggleTruncate}">${options.lessText}</span></span></span>`);

        // Now managed by delegation handler in base.js
        // el.find(`.${options.toggleTruncate}-link`).click(function() {
        //     el.find(`.${options.toggleTruncate}-wrapper`).toggleClass(`${options.toggleTruncate}-more`);
        // });
    });
};
